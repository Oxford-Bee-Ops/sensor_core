# type: ignore
####################################################################################################
# Class: AudioProcessor
#
# This class is used on the Raspberry Pi to process the audio data from the microphone.
#
# For hive-entrance audio, it is intended to act as a "trap camera" for audio, detecting buzzing
# from bees as they enter and leave the hive.
# The output is a spectrogram representing 2s segments if there is a bee buzzing event.
# We also save a random subset of original "long" form audio files for testing purposes to the cloud.
#
# We also receive in-hive recordings that we don't process here, but pass up to the cloud for 
# further processing. We mark these files by adding a "inhive" flag to the filename but otherwise 
# treat them as long-form samples.
#
# There are two classes:
# - AudioProcessor:
#   - This is a wrapper that processes mulitple audio files.
#   - It is responsible for saving in-hive and long-form audio files to the cloud.
# - AudioFileProcessor:
#   - This class processes individual audio files if they are hive-entrance recordings.
####################################################################################################
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import scipy.io.wavfile as wav
from scipy import signal
from sensors.sensor_audio import AudioSensorCfg

from sensor_core import DataProcessor, DataProcessorCfg, Datastream, DpContext, api
from sensor_core import configuration as root_cfg
from sensor_core.utils import audio_transforms as at
from sensor_core.utils import utils

logger = root_cfg.setup_logger("sensor_core")

#############################################################################################################
# Define the DataProcessorCfg objects
#############################################################################################################
@dataclass
class AudioProcessorCfg(DataProcessorCfg):
    #######################################################################
    # Standard DataProcessorCfg fields
    #######################################################################
    dp_class_ref: str = "sensor_core.sensors.processor_audio_hive.HiveAudioProcessor"
    dp_description: str = "HiveAudio processor for hive entrance audio"
    input_format: api.FILE_FORMATS = "wav"
    output_format: api.FILE_FORMATS = "png"
    ########################################################################
    # Custom fields
    ########################################################################


class HiveAudioProcessor(DataProcessor):

    # Process a list of audio files
    def process_data(self, 
                     datastream: Datastream, 
                     input_data: pd.DataFrame | list[Path], 
                     context: DpContext) -> Optional[pd.DataFrame]:
        
        # We only ever expect to be passed a list of files
        assert isinstance(input_data, list)
        files: list[Path] = input_data
        assert isinstance(context.sensor, AudioSensorCfg)
        sensor_cfg: AudioSensorCfg = context.sensor
        in_hive_port = sensor_cfg.in_hive_mic_port
        port = datastream.sensor_index

        events_written = 0

        # Loop through the files, and process each one
        # Most files
        for f in files:
            # We wrap this in a try-except because if we get one corrupt file, we don't want to 
            # stop processing all files
            try:
                # If we are doing in-hive recordings, we need to check whether this file is from 
                # an in-hive recording
                if (in_hive_port == 0) or (port != in_hive_port):
                    # This is a regular hive entrance recording, so load and process it
                    wav_helper = AudioFileProcessor(f, datastream, in_hive_port)
                    saved_image_fnames = wav_helper.write_event_spectrograms()
                    events_written += len(saved_image_fnames)

            except Exception as e:
                logger.error(
                    root_cfg.RAISE_WARN() + "%s Exception occurred in audio processing of %s, exception: %s",
                    root_cfg.my_device_id,
                    str(f),
                    str(e),
                    exc_info=True,
                )

        # Calculate the percent_written
        # 2s clips from 3 minute audio files
        sample_duration = root_cfg.my_device.av_rec_seconds * len(files)
        subsample_duration = events_written * AudioFileProcessor.RESAMPLE_LENGTH
        0 if sample_duration == 0 else int(subsample_duration / sample_duration)
        # @@@ Where do we save this data?


class AudioFileProcessor:
    MAX_FREQ = 2400  # Max frequency to save in spectrograms

    # The initialisation loads the wav file
    def __init__(self, wav_fname: Path, datastream: Datastream, in_hive_port=0):
        self.wav_fname = wav_fname
        self.datastream = datastream
        self.in_hive_port = in_hive_port
        self.port = datastream.sensor_index
        self.display_name = wav_fname.name

        # Read the wav file
        try:
            self.samplerate, self.data = wav.read(wav_fname)
        except ValueError as e:
            raise ValueError(f"ERROR: Unable to read wav file: {wav_fname} - {e!s}")

        # Throw an exception if we have less than a second of audio because this is
        # probably a corrupt file and will throw an error later when we do SFFT
        if len(self.data) < self.samplerate:
            raise ValueError(f"ERROR: Wav file is too short: {wav_fname}")

        self.duration = int(len(self.data) / self.samplerate) + 1

        # Choose an appropriate nperseg for spectrograms based on the sample rate
        if self.samplerate > 30000:
            self.nperseg = 2048
        elif self.samplerate > 15000:
            self.nperseg = 1024
        elif self.samplerate > 7500:
            self.nperseg = 512

        # Clean bogus data that is outside the 16 bit integer range and sometimes present in the wav file
        # self.data[self.data>32766] = 32766 # 16 bit signed integer max value
        self.data[self.data < -32766] = -32766  # 16 bit signed integer min value

        # Extract audio events
        self.extract_audio_events_harmonics(self.datastream)

    ########################################################
    # AUDIO-BASED EVENT EXTRACTION FUNCTIONS
    ########################################################

    # Function to extract audio events from the wav file.
    #
    # The fundamentals behind this model are that:
    #  - bee buzzing is a harmonic sound, unlike most other sounds in the environment
    #  - the fundamental frequency of bee buzzing is ~190Hz, so discard frequencies below 150Hz
    #  - the buzzing will be reasonably loud near the mic relative to the background
    #  - the 2nd harmonic has the most power
    #  - buzzing can we brief on exit (0.3s) but long enough to expect some consistency over timeslices
    #
    # This was inspired by Heise et al, 2017 (10.1109/SAS.2017.7894089)
    #
    # We also use a "good enough" matching algorithm to identify events:
    # a possible event doesn't have to meet all the criteria, it just has to match on all bar 1.
    P0_LOW_BAND_ABOVE_140 = 0
    P1_HARMONIC_1 = 1
    P2_HARMONIC_3 = 2
    P3_HARMONIC_4 = 3
    P4_HARMONIC2_IS_MAX = 4
    P5_INTER_HARMONIC_2 = 5
    P6_INTER_HARMONIC_3 = 6
    P7_INTER_HARMONIC_4 = 7
    P8_NEXT_TIME_H2_SIMILAR = 8
    P9_POWER_PEAK = 9

    PARAMETER_WEIGHTS = np.array(
        [
            [P0_LOW_BAND_ABOVE_140, 0.75],
            [P1_HARMONIC_1, 0.5],
            [P2_HARMONIC_3, 1.5],
            [P3_HARMONIC_4, 1.5],
            [P4_HARMONIC2_IS_MAX, 1],
            [P5_INTER_HARMONIC_2, 0.5],
            [P6_INTER_HARMONIC_3, 0.5],
            [P7_INTER_HARMONIC_4, 0.5],
            [P8_NEXT_TIME_H2_SIMILAR, 1.5],
            [P9_POWER_PEAK, 1.5],
        ]
    )
    MAX_SCORE = np.sum(PARAMETER_WEIGHTS[:, 1])
    # We've lowered this from the normal 0.6, because we want to be a bit trigger 
    # happy rather than miss things.
    SCORE_THRESHOLD = MAX_SCORE * 0.6

    P_THRESH = np.array(
        [
            140,  # P0_LOW_BAND_ABOVE_140
            0.075,  # P1_HARMONIC_1
            0.075,  # P2_HARMONIC_3
            0.075,  # P3_HARMONIC_4
            0,  # P4_HARMONIC2_IS_MAX
            1,  # P5_INTER_HARMONIC_2
            1,  # P6_INTER_HARMONIC_3
            1,  # P7_INTER_HARMONIC_4
            0.075,  # P8_NEXT_TIME_H2_SIMILAR
            2,  # P9_POWER_PEAK
        ]
    )

    # We need to report via our TELEM# logs the duration of sampling and whether we chose to save the sample
    SAMPLE_OUTCOME_SUCCESS = "success"
    SAMPLE_OUTCOME_BAD_SENSOR = "bad_sensor"
    SAMPLE_OUTCOME_FAIL = "fail"
    # When sensors go bad, they generate lots of static which has a high mean power
    # We reject any audio files with a mean power higher than the MAX_MEAN_SPRECTROGRAM_POWER
    MAX_MEAN_SPRECTROGRAM_POWER = 10

    def extract_audio_events_harmonics(self, datastream: Datastream):
        
        # Assume success
        self.sample_outcome = AudioFileProcessor.SAMPLE_OUTCOME_SUCCESS

        # Create an array of seconds during the recording to track events
        self.audio_event_mask = np.zeros(self.duration)

        # Create the spectrogram
        # We use a slightly longer nperseg to get a better frequency resolution and improve sensitivity*
        # *Test example that fails with a lower nperseg is 
        # 2023-07-07T10_10_02.084.e.2023-07-07T10_15_02.085.wav
        frequencies, times, spectrogram = signal.spectrogram(
            self.data, self.samplerate, nperseg=self.nperseg * 2
        )
        self.event_extraction_frequency_resolution = frequencies[1] - frequencies[0]
        self.event_extraction_time_resolution = times[1] - times[0]

        # Converting to a log scale is only required for the power threshold check below - 
        # also makes display clearer
        # Could be optimised out.
        spectrogram = np.log(spectrogram)

        # We need to use the frequencies array to work out the index into the spectrogram 
        # corresponding to our desired Hz ranges
        # We've opted for these ranges because sample data suggests the fundamental frequency is 160-200Hz
        target_hz = 180
        base = target_hz / 2
        hz_bands = [
            0,
            base + target_hz * 1,
            base + target_hz * 2,
            base + target_hz * 3,
            base + target_hz * 4,
        ]
        f_bins = [int(hz / frequencies[1]) for hz in hz_bands]
        time_slice_per_s = int(1 / times[1])

        # Now loop through the spectrogram and record in an array the max power in each of the 
        # four frequency ranges
        # The array should have times as the first dimension and the four frequency ranges as the 
        # second dimension We use the freq_300, freq_500, freq_700 and freq_900 to index into the spectrogram
        max_power_array = np.zeros((len(spectrogram[0]), 4))
        max_freq_array = np.zeros((len(spectrogram[0]), 4))
        paramater_array = np.zeros((len(spectrogram[0]), len(AudioFileProcessor.PARAMETER_WEIGHTS)))
        scores = np.zeros(len(spectrogram[0]))

        for i in range(len(spectrogram[0])):
            # Get the frequency with max power in each of the 4 frequency ranges and store in the array
            for bands in range(4):
                i_hz = np.argmax(spectrogram[f_bins[bands] : f_bins[bands + 1], i])
                max_freq_array[i, bands] = frequencies[f_bins[bands] + i_hz]
                max_power_array[i, bands] = spectrogram[f_bins[bands] + i_hz, i]
            i += 1

        # Save the max log power in the spectrogram for later use
        self.max_log_spectrogram_power = float(np.max(max_power_array))
        self.mean_log_spectrogram_power = float(np.mean(max_power_array))

        # Check that the mean power in the spectrogram is not too high
        if self.mean_log_spectrogram_power > AudioFileProcessor.MAX_MEAN_SPRECTROGRAM_POWER:
            self.sample_outcome = AudioFileProcessor.SAMPLE_OUTCOME_BAD_SENSOR
        else:
            # Iterate over the spectrogram to find 1s periods where there is an event
            for i in range(len(spectrogram[0])):
                # We use a points-based "good enough" algorithm to identify events
                # A timeslices scores 1 point for each match and needs to get 9/10
                # We check that each sample period is above the log power threshold
                if max_power_array[i, 1] > 0:
                    score = 0
                    # P0: Check that the fundamental frequency of the lowest band is > 150Hz
                    if (
                        max_freq_array[i, 0]
                        > AudioFileProcessor.P_THRESH[AudioFileProcessor.P0_LOW_BAND_ABOVE_140]
                    ):
                        paramater_array[i, AudioFileProcessor.P0_LOW_BAND_ABOVE_140] = 1
                    # P1-3: Test whether a multiple of the fundamental frequency is within 5% in the 
                    # other three ranges
                    # We use the 2nd harmonic as the base/2 because it's generally the strongest
                    # We need to divide the frequency in each range by the fundamental frequency
                    # We then need to check that the result is within 5% of an integer
                    # We therefore need to divide by the fundamental frequency and then subtract 
                    # the nearest integer
                    # We can then test the remainder is within 5% of 0
                    fundamental_freq = max_freq_array[i, 1] / 2
                    if (
                        abs(
                            (max_freq_array[i, 0] / fundamental_freq)
                            - round(max_freq_array[i, 0] / fundamental_freq)
                        )
                        < AudioFileProcessor.P_THRESH[AudioFileProcessor.P1_HARMONIC_1]
                    ):
                        paramater_array[i, AudioFileProcessor.P1_HARMONIC_1] = 1
                    if (
                        abs(
                            (max_freq_array[i, 2] / fundamental_freq)
                            - round(max_freq_array[i, 2] / fundamental_freq)
                        )
                        < AudioFileProcessor.P_THRESH[AudioFileProcessor.P2_HARMONIC_3]
                    ):
                        paramater_array[i, AudioFileProcessor.P2_HARMONIC_3] = 1
                    if (
                        abs(
                            (max_freq_array[i, 3] / fundamental_freq)
                            - round(max_freq_array[i, 3] / fundamental_freq)
                        )
                        < AudioFileProcessor.P_THRESH[AudioFileProcessor.P3_HARMONIC_4]
                    ):
                        paramater_array[i, AudioFileProcessor.P3_HARMONIC_4] = 1

                    # P4: Check that the 2nd harmonic is the strongest
                    if (
                        (max_power_array[i, 1] > max_power_array[i, 0])
                        and (max_power_array[i, 1] > max_power_array[i, 2])
                        and (max_power_array[i, 1] > max_power_array[i, 3])
                    ):
                        paramater_array[i, AudioFileProcessor.P4_HARMONIC2_IS_MAX] = 1

                    # P5-6: Check that the inter-harmonic frequencies are not too strong
                    # We test 3 bands
                    inter_harm_freqs = [
                        fundamental_freq * 1.5,
                        fundamental_freq * 2.5,
                        fundamental_freq * 3.5,
                    ]
                    inter_harm_f_bins = [int(freq / frequencies[1]) for freq in inter_harm_freqs]
                    # Now check that the power in the inter-harmonic frequency is at least 2 orders 
                    # of magnitude lower than the harmonic
                    if (
                        spectrogram[f_bins[1], i] - spectrogram[inter_harm_f_bins[0], i]
                    ) > AudioFileProcessor.P_THRESH[AudioFileProcessor.P5_INTER_HARMONIC_2]:
                        paramater_array[i, AudioFileProcessor.P5_INTER_HARMONIC_2] = 1
                    if (
                        spectrogram[f_bins[2], i] - spectrogram[inter_harm_f_bins[1], i]
                    ) > AudioFileProcessor.P_THRESH[AudioFileProcessor.P6_INTER_HARMONIC_3]:
                        paramater_array[i, AudioFileProcessor.P6_INTER_HARMONIC_3] = 1
                    if (
                        spectrogram[f_bins[3], i] - spectrogram[inter_harm_f_bins[2], i]
                    ) > AudioFileProcessor.P_THRESH[AudioFileProcessor.P7_INTER_HARMONIC_4]:
                        paramater_array[i, AudioFileProcessor.P7_INTER_HARMONIC_4] = 1

                    # P8: Now check that the 2nd harmonic of the next timeslice is similar
                    if i < len(spectrogram[0]) - 1:
                        if (
                            abs((max_freq_array[i + 1, 1] / max_freq_array[i, 1]) - 1)
                            < AudioFileProcessor.P_THRESH[AudioFileProcessor.P8_NEXT_TIME_H2_SIMILAR]
                        ):
                            paramater_array[i, AudioFileProcessor.P8_NEXT_TIME_H2_SIMILAR] = 1

                    # P9: Check that the power is at a peak relative to the preceding second
                    # Calculate the average power in the 4 frequency bands over the preceding second
                    # We need to work out how many timeslices in the preceding second
                    if i > 1:
                        t_i = max(0, i - time_slice_per_s - 1)
                        if (
                            np.mean(max_power_array[i, :]) - np.mean(max_power_array[t_i : i - 1, :])
                        ) > AudioFileProcessor.P_THRESH[AudioFileProcessor.P9_POWER_PEAK]:
                            paramater_array[i, AudioFileProcessor.P9_POWER_PEAK] = 1

                    # We calculate the score by multiplying the parameter array by the parameter 
                    # weights and summing the result
                    score = np.sum(paramater_array[i, :] * AudioFileProcessor.PARAMETER_WEIGHTS[:, 1])
                    scores[i] = score
                    if score >= AudioFileProcessor.SCORE_THRESHOLD:
                        # We have found a buzzing noise that is above threshold.  Mark it as an event.
                        # Use the times array to work out the index into the audio_event_mask
                        self.audio_event_mask[int(times[i])] = 1

        # Save the scores array - we use this in the audio event extraction
        assert len(scores) == len(times), (
            "ERROR: scores and times arrays are different lengths: "
            + str(len(scores))
            + " vs "
            + str(len(times))
        )
        self.scores = scores

        datastream.log({
            "port": self.port,
            "filename": self.display_name,
            "sample_duration": self.duration,
            "sample_outcome": self.sample_outcome,
            "sample_rate": str(self.samplerate),
            "event_seconds": str(sum(self.audio_event_mask)),
            "mean_scores": "{:.3f}".format(float(np.mean(self.scores))),
            "mean_power": "{:.3f}".format(self.mean_log_spectrogram_power),
            "max_power": "{:.3f}".format(self.max_log_spectrogram_power),
            "score_threshold": str(AudioFileProcessor.SCORE_THRESHOLD),
            "event_extraction_time_resolution": "{:.3f}".format(self.event_extraction_time_resolution),
        })

    # Function to return the audio_event_mask (primarily for testing purposes)
    def get_audio_event_mask(self, print_mask=True):
        if print_mask:
            for i in range(len(self.audio_event_mask)):
                print(str(int(self.audio_event_mask[i])) + ",", end="")
            print("])")

        return self.audio_event_mask

    # Returns a list of events in the audio file
    # Events are a (start,end) tuple in seconds since the start of the file
    def get_events(self):
        # Iterate over the audio_event_mask and write out all continuous blocks
        i = 0
        events = []
        while i < self.duration:
            if self.audio_event_mask[i] > 0:
                # Create a wav file and include the preceding second
                start = max(0, i - 1)
                # Skip forward to the end of the event
                while (i < self.duration) and (self.audio_event_mask[i] > 0):
                    i += 1
                # Include the second after the event
                end = min(i + 1, self.duration)
                events.append((start, end))
            else:
                i += 1
        return events

    # Subsample an event to a 2s period to create a standardised clip spectrogram
    RESAMPLE_LENGTH = 2  # The length of the resampled clip in s

    def subsample_event(self, event):
        time_slice_length = self.event_extraction_time_resolution
        window_len = int((0.8 * AudioFileProcessor.RESAMPLE_LENGTH) / time_slice_length)
        max_score = 0
        max_i = 0

        # Work out the index into the scores array for the event
        # Index = time into the event * time_slice_length
        start, end = event
        start_i = int(start / time_slice_length)
        end_i = int(end / time_slice_length)

        # To avoid looping off the end of the scores array, we need to ensure we
        # subtract the window length from the end_i, and stay < len(scores)
        end_i_range = min(end_i + 1, len(self.scores) - window_len)
        for i in range(start_i, end_i_range):
            score = np.sum(self.scores[i : i + window_len])
            if score > max_score:
                max_score = score
                max_i = i

        # Calculate the start and end indices into the raw wav data array
        # To do so, we convert max_i to the number-of-samples-into-the-wav-file
        # We then start 0.2s earlier if possible and finish 0.2s later if possible
        time_since_start_in_s = max_i * time_slice_length
        shifted_start = max(
            time_since_start_in_s - 0.2, 0
        )  # We don't want to shift off the beginning of the wav sample
        subsample_start = int(shifted_start * self.samplerate)
        subsample_end = subsample_start + int(self.samplerate * AudioFileProcessor.RESAMPLE_LENGTH)
        if subsample_end > len(self.data):  # We don't want to go off the end of the wav sample
            subsample_end = len(self.data) - 1
            subsample_start = max(
                subsample_end - int(self.samplerate * AudioFileProcessor.RESAMPLE_LENGTH),
                0,
            )

        return subsample_start, subsample_end

    ########################################################
    # Code to write out either detected events or arbitrary clips
    ########################################################

    # Function to save spectrograms of detected events (these are 30x smaller than the wav files)
    # This code looks for the most interesting 2s period in the identified event
    # and converts that to a spectrogram, saving it to the tmp/audio directory.
    def write_event_spectrograms(self, output_dir=root_cfg.AUDIO_UPLOAD_DIR) -> list[Path]:
        saved_fft_images: list[Path] = []
        # Use an iterator to loop through the file and get the start and end of each event
        events = self.get_events()
        for event in events:
            start, end = self.subsample_event(event)

            # Bit ugly writing a file (in memory) and then deleting it, but it's what the torchaudio library
            # expects
            event_fname = self.write_wav(output_dir, start, end)
            try:
                image_fname = at.save_wav_as_image(event_fname)
                saved_fft_images.append(image_fname)
            except Exception as e:
                logger.error(
                    root_cfg.RAISE_WARN()
                    + " Error creating spectrogram image for "
                    + event_fname
                    + ": "
                    + str(e),
                    exc_info=True,
                )
                # If we fail here, we delete the wav file and move on to the next one
            os.remove(event_fname)
        return saved_fft_images

    # Function to save the audio for detected events as clips
    def write_event_wav_files(self, output_dir=root_cfg.AUDIO_UPLOAD_DIR):
        wav_s_written = 0

        # Iterate over the audio_event_mask and write out all continuous blocks
        events = self.get_events()
        for event in events:
            start, end = event

            # Calculate the start and end indices into the raw wav data array
            start_i = start * self.samplerate
            end_i = min(len(self.data) - 1, end * self.samplerate)
            self.write_wav(output_dir, start_i, end_i)
            wav_s_written += end - start

        # Return the % of the wav file that was written out
        return int(100 * wav_s_written / self.duration)

    # Function to write out a wav file
    # Start and end are the indices into the raw wav data array
    def write_wav(self, output_dir: Path, start, end):
        # Update the output file name to be in the right directory and have the absolute start and 
        # end times of the sample
        sample_fname = output_dir.joinpath(self.wav_fname.name)
        t1, _ = utils.get_timestamps_from_fname(sample_fname)
        abs_start_time = datetime.fromisoformat(t1).timestamp()
        sample_fname = utils.update_start_timestamp(sample_fname, abs_start_time + (start / self.samplerate))
        sample_fname = utils.update_end_timestamp(sample_fname, abs_start_time + (end / self.samplerate))

        # If the tmp/audio directory doesn't exist, create it
        if not os.path.exists(output_dir):
            logger.warning(f"Had to recreate audio directory: {output_dir}")
            os.makedirs(output_dir)

        # If the file already exists, overwrite it
        if os.path.exists(sample_fname):
            os.remove(sample_fname)

        # Write out the wav file
        wav.write(sample_fname, self.samplerate, self.data[start:end])

        return sample_fname
