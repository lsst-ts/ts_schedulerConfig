import lsst.pex.config as pexConfig

from lsst.ts.schedulerConfig.proposal import BandFilter, MasterSubSequence, SequenceScheduling
from lsst.ts.schedulerConfig.proposal import SkyConstraints, SkyExclusion, SkyNightlyBounds, SubSequence

__all__ = ["Sequence"]

class Sequence(pexConfig.Config):
    """Configuration for a sequence proposal. This includes sequence, sub-sequence and
       nested sub-sequence proposals.
    """

    name = pexConfig.Field('Name for the proposal.', str)
    sky_user_regions = pexConfig.ListField('Sky user regions for the proposal as a list of field Ids.', int)
    sky_exclusion = pexConfig.ConfigField('Sky region selection for the proposal.', SkyExclusion)
    sky_nightly_bounds = pexConfig.ConfigField('Sky region selection for the proposal.', SkyNightlyBounds)
    sky_constraints = pexConfig.ConfigField('Sky region selection for the proposal.', SkyConstraints)
    sub_sequences = pexConfig.ConfigDictField('Set of sub-sequences.', int, SubSequence)
    master_sub_sequences = pexConfig.ConfigDictField('Set of master sub-sequences.', int, MasterSubSequence)
    filters = pexConfig.ConfigDictField('Filter configuration for the proposal.', str, BandFilter)
    scheduling = pexConfig.ConfigField('Scheduling configuration for the proposal.', SequenceScheduling)

    def setDefaults(self):
        """Default specification for a sequence proposal.
        """
        self.sky_user_regions = []
        self.sub_sequences = {}
        self.master_sub_sequences = {}

    def proposal_fields(self):
        """Return the list of field Ids for this proposal.

        Returns
        -------
        list[int]
        """
        return sorted(self.sky_user_regions)

    def set_topic(self, topic):
        """Set the information on a DDS topic instance.

        Parameters
        ----------
        topic : SALPY_scheduler.scheduler_sequencePropConfigC
            The instance of the DDS topic to set information on.

        Returns
        -------
        SALPY_scheduler.scheduler_sequencePropConfigC
            The topic with current information set.
        """
        topic.name = self.name if self.name is not None else "None"

        topic.twilightBoundary = self.sky_nightly_bounds.twilight_boundary
        topic.deltaLst = self.sky_nightly_bounds.delta_lst
        topic.decWindow = self.sky_exclusion.dec_window
        topic.maxAirmass = self.sky_constraints.max_airmass
        topic.maxCloud = self.sky_constraints.max_cloud
        topic.minDistanceMoon = self.sky_constraints.min_distance_moon
        topic.excludePlanets = self.sky_constraints.exclude_planets

        num_sky_user_regions = len(self.sky_user_regions)
        topic.numUserRegions = num_sky_user_regions
        for i, sky_user_region in enumerate(self.sky_user_regions):
            topic.userRegionIds[i] = sky_user_region

        num_sub_sequences = len(self.sub_sequences) if self.sub_sequences is not None else 0
        topic.numSubSequences = num_sub_sequences
        if topic.numSubSequences:
            sub_sequence_names = []
            sub_sequence_filters = []
            filter_visit_index = 0
            for i, sub_sequence in self.sub_sequences.items():
                sub_sequence_names.append(sub_sequence.name)
                sub_sequence_filters.append(sub_sequence.get_filter_string())
                topic.numSubSequenceFilters[i] = len(sub_sequence.filters)
                for filter_visit in sub_sequence.visits_per_filter:
                    topic.numSubSequenceFilterVisits[filter_visit_index] = filter_visit
                    filter_visit_index += 1
                topic.numSubSequenceEvents[i] = sub_sequence.num_events
                topic.numSubSequenceMaxMissed[i] = sub_sequence.num_max_missed
                topic.subSequenceTimeIntervals[i] = sub_sequence.time_interval
                topic.subSequenceTimeWindowStarts[i] = sub_sequence.time_window_start
                topic.subSequenceTimeWindowMaximums[i] = sub_sequence.time_window_max
                topic.subSequenceTimeWindowEnds[i] = sub_sequence.time_window_end
                topic.subSequenceTimeWeights[i] = sub_sequence.time_weight

            topic.subSequenceNames = ",".join(sub_sequence_names)
            topic.subSequenceFilters = ",".join(sub_sequence_filters)

        num_master_sub_sequences = len(self.master_sub_sequences) \
            if self.master_sub_sequences is not None else 0
        topic.numMasterSubSequences = num_master_sub_sequences
        if topic.numMasterSubSequences:
            master_sub_sequence_names = []
            nested_sub_sequence_names = []
            nested_sub_sequence_filters = []
            nss_index = 0
            filter_visit_index = 0
            for i, master_sub_sequence in self.master_sub_sequences.items():
                master_sub_sequence_names.append(master_sub_sequence.name)
                topic.numNestedSubSequences[i] = len(master_sub_sequence.sub_sequences)
                topic.numMasterSubSequenceEvents[i] = master_sub_sequence.num_events
                topic.numMasterSubSequenceMaxMissed[i] = master_sub_sequence.num_max_missed
                topic.masterSubSequenceTimeIntervals[i] = master_sub_sequence.time_interval
                topic.masterSubSequenceTimeWindowStarts[i] = master_sub_sequence.time_window_start
                topic.masterSubSequenceTimeWindowMaximums[i] = master_sub_sequence.time_window_max
                topic.masterSubSequenceTimeWindowEnds[i] = master_sub_sequence.time_window_end
                topic.masterSubSequenceTimeWeights[i] = master_sub_sequence.time_weight
                for sub_sequence in master_sub_sequence.sub_sequences.values():
                    nested_sub_sequence_names.append(sub_sequence.name)
                    nested_sub_sequence_filters.append(sub_sequence.get_filter_string())
                    topic.numNestedSubSequenceFilters[nss_index] = len(sub_sequence.filters)
                    for filter_visit in sub_sequence.visits_per_filter:
                        topic.num_nested_sub_sequence_filter_visits[filter_visit_index] = filter_visit
                        filter_visit_index += 1
                    topic.numNestedSubSequenceEvents[nss_index] = sub_sequence.num_events
                    topic.numNestedSubSequenceMaxMissed[nss_index] = sub_sequence.num_max_missed
                    topic.nestedSubSequenceTimeIntervals[nss_index] = sub_sequence.time_interval
                    topic.nestedSubSequenceTimeWindowStarts[nss_index] = sub_sequence.time_window_start
                    topic.nestedSubSequenceTimeWindowMaximums[nss_index] = sub_sequence.time_window_max
                    topic.nestedSubSequenceTimeWindowEnds[nss_index] = sub_sequence.time_window_end
                    topic.nestedSubSequenceTimeWeights[nss_index] = sub_sequence.time_weight
                    nss_index += 1

            topic.masterSubSequenceNames = ",".join(master_sub_sequence_names)
            topic.nestedSubSequenceNames = ",".join(nested_sub_sequence_names)
            topic.nestedSubSequenceFilters = ",".join(nested_sub_sequence_filters)

        topic.numFilters = len(self.filters) if self.filters is not None else 0
        if topic.numFilters:
            filter_names = []
            exp_index = 0
            for i, v in enumerate(self.filters.values()):
                filter_names.append(v.name)
                topic.brightLimit[i] = v.bright_limit
                topic.darkLimit[i] = v.dark_limit
                topic.maxSeeing[i] = v.max_seeing
                topic.numFilterExposures[i] = len(v.exposures)
                for exposure in v.exposures:
                    topic.exposures[exp_index] = exposure
                    exp_index += 1
            topic.filterNames = ','.join(filter_names)

        topic.maxNumTargets = self.scheduling.max_num_targets
        topic.acceptSerendipity = self.scheduling.accept_serendipity
        topic.acceptConsecutiveVisits = self.scheduling.accept_consecutive_visits
        topic.airmassBonus = self.scheduling.airmass_bonus
        topic.hourAngleBonus = self.scheduling.hour_angle_bonus
        topic.hourAngleMax = self.scheduling.hour_angle_max
        topic.restartLostSequences = self.scheduling.restart_lost_sequences
        topic.restartCompleteSequences = self.scheduling.restart_complete_sequences
        topic.maxVisitsGoal = self.scheduling.max_visits_goal

        return topic
