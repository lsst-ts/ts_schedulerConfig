import lsst.pex.config as pexConfig

from lsst.ts.schedulerConfig.proposal import GeneralBandFilter, GeneralScheduling
from lsst.ts.schedulerConfig.proposal import SkyConstraints, SkyExclusion, SkyNightlyBounds, SkyRegion

__all__ = ["General"]

class General(pexConfig.Config):
    """Configuration for a general proposal. This includes area distribution, time-domain
       and hybrid proposals.
    """

    name = pexConfig.Field('Name for the proposal.', str)
    sky_region = pexConfig.ConfigField('Sky region selection for the proposal.', SkyRegion)
    sky_exclusion = pexConfig.ConfigField('Sky region selection for the proposal.', SkyExclusion)
    sky_nightly_bounds = pexConfig.ConfigField('Sky region selection for the proposal.', SkyNightlyBounds)
    sky_constraints = pexConfig.ConfigField('Sky region selection for the proposal.', SkyConstraints)
    filters = pexConfig.ConfigDictField('Filter configuration for the proposal.', str, GeneralBandFilter)
    scheduling = pexConfig.ConfigField('Scheduling configuration for the proposal.', GeneralScheduling)

    def proposal_fields(self, fd, fs):
        """Return the field Ids for this proposal.

        Parameters
        ----------
        fd : lsst.sims.survey.fields.FieldsDatabase
            An instance of the fields database.
        fs : lsst.sims.survey.fields.FieldSelection
            An instance of the field selector.

        Returns
        -------
        list[int]
        """
        query_list = []
        combine_list = []
        region_cuts = []

        # Handle any time dependent cuts
        try:
            num_selections = len(self.sky_region.selection_mapping)
            for i, mapping in self.sky_region.selection_mapping.items():
                for index in mapping.indexes:
                    region_cuts.append(self.sky_region.selections[index])
                try:
                    combine_list.append(self.sky_region.combiners[i])
                except IndexError:
                    # Don't have combiners, must be single selection per time range
                    pass
                if i < num_selections - 1:
                    combine_list.append("or")
        except TypeError:
            region_cuts = list(self.sky_region.selections.values())
            combine_list.extend(self.sky_region.combiners)

        # Handle the sky region selections
        for cut in region_cuts:
            cut_type = cut.limit_type
            if cut_type != "GP":
                query_list.append(fs.select_region(cut_type, cut.minimum_limit, cut.maximum_limit))
            else:
                query_list.append(fs.galactic_region(cut.maximum_limit, cut.minimum_limit,
                                                     cut.bounds_limit))

        # Handle the sky exclusion selections
        exclusion_query = None
        for cut in self.sky_exclusion.selections.values():
            cut_type = cut.limit_type
            if cut_type == "GP":
                # Need the field Ids, so don't mark it as an exclusion
                exclusion_query = fs.galactic_region(cut.maximum_limit, cut.minimum_limit,
                                                     cut.bounds_limit)

        query = fs.combine_queries(*query_list, combiners=combine_list)
        fields = fd.get_field_set(query)
        ids = set([x[0] for x in fields])
        if exclusion_query is not None:
            equery = fs.combine_queries(exclusion_query)
            efields = fd.get_field_set(equery)
            eids = set([x[0] for x in efields])
            ids.difference_update(eids)

        return sorted(list(ids))

    def set_topic(self, topic):
        """Set the information on a DDS topic instance.

        Parameters
        ----------
        topic : SALPY_scheduler.scheduler_generalPropConfigC
            The instance of the DDS topic to set information on.

        Returns
        -------
        SALPY_scheduler.scheduler_generalPropConfigC
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

        num_region_selections = len(self.sky_region.selections) \
            if self.sky_region.selections is not None else 0
        topic.numRegionSelections = num_region_selections
        if num_region_selections:
            limit_types = []
            for i, v in enumerate(self.sky_region.selections.values()):
                limit_types.append(v.limit_type)
                topic.regionMinimums[i] = v.minimum_limit
                topic.regionMaximums[i] = v.maximum_limit
                topic.regionBounds[i] = v.bounds_limit
            topic.regionTypes = ','.join(limit_types)

        topic.regionCombiners = ','.join(self.sky_region.combiners)

        num_time_ranges = len(self.sky_region.time_ranges) if self.sky_region.time_ranges is not None else 0
        topic.numTimeRanges = num_time_ranges
        if num_time_ranges:
            for i, v in enumerate(self.sky_region.time_ranges.values()):
                topic.timeRangeStarts[i] = v.start
                topic.timeRangeEnds[i] = v.end

        num_selection_mappings = len(self.sky_region.selection_mapping) \
            if self.sky_region.selection_mapping is not None else 0
        if num_selection_mappings:
            selection_index = 0
            for i, v in enumerate(self.sky_region.selection_mapping.values()):
                topic.numSelectionMappings[i] = len(v.indexes)
                for index in v.indexes:
                    topic.selectionMappings[selection_index] = index
                    selection_index += 1

        num_exclusion_selections = len(self.sky_exclusion.selections) \
            if self.sky_exclusion.selections is not None else 0
        topic.numExclusionSelections = num_exclusion_selections
        if num_exclusion_selections:
            limit_types = []
            for i, v in enumerate(self.sky_exclusion.selections.values()):
                limit_types.append(v.limit_type)
                topic.exclusionMinimums[i] = v.minimum_limit
                topic.exclusionMaximums[i] = v.maximum_limit
                topic.exclusionBounds[i] = v.bounds_limit
            topic.exclusionTypes = ','.join(limit_types)

        topic.numFilters = len(self.filters) if self.filters is not None else 0
        if topic.numFilters:
            filter_names = []
            exp_index = 0
            for i, v in enumerate(self.filters.values()):
                filter_names.append(v.name)
                topic.numVisits[i] = v.num_visits
                topic.numGroupedVisits[i] = v.num_grouped_visits
                topic.maxGroupedVisits[i] = v.max_grouped_visits
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
        topic.restrictGroupedVisits = self.scheduling.restrict_grouped_visits
        topic.timeInterval = self.scheduling.time_interval
        topic.timeWindowStart = self.scheduling.time_window_start
        topic.timeWindowMax = self.scheduling.time_window_max
        topic.timeWindowEnd = self.scheduling.time_window_end
        topic.timeWeight = self.scheduling.time_weight
        topic.fieldRevisitLimit = self.scheduling.field_revisit_limit

        return topic
