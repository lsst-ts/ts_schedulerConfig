from builtins import object
import logging
import time

from lsst.ts.schedulerConfig.utilities.socs_exceptions import SchedulerTimeoutError

__all__ = ["ConfigurationCommunicator"]

class ConfigurationCommunicator(object):
    """Main class for configuration communication.

    This class handles setting up the configuration DDS topics and publishing them so they can be picked up
    by the Scheduler.

    Attributes
    ----------
    sal : :class:`.SalManager`
        The object responsible for SAL interaction.
    config: :class:`.SimulationConfig`
        The top-level simulation configuration object.
    log : logging.Logger
        The logging instance.
    """

    def __init__(self, no_dds_comm=False):
        """Initialize the class.
        """
        self.sal = None
        self.config = None
        self.log = logging.getLogger("configuration.ConfigurationCommunicator")
        self.no_dds_comm = no_dds_comm
        self.socs_timeout = 180.0  # seconds
        self.survey_topology = {}

    def initialize(self, sal, config):
        """Perform initialization steps.

        Parameters
        ----------
        sal : :class:`.SalManager`
            The instance responsible for SAL interaction.
        config : :class:`.SimulationConfig`
            The top-level simulation configuration instance.
        """
        self.log.info("Initializing configuration communication")
        self.sal = sal
        self.config = config
        self.configure()

    def _configure_scheduler(self):
        """Configure and send the Scheduler configuration topic.
        """
        if self.no_dds_comm:
            from SALPY_scheduler import scheduler_schedulerConfigC
            self.sched_conf = scheduler_schedulerConfigC()
        else:
            self.sched_conf = self.sal.set_subscribe_topic("schedulerConfig")

            lasttime = time.time()
            while True:
                rcode = self.sal.manager.getNextSample_schedulerConfig(self.sched_conf)
                self.log.debug('[rcode:%i] - Listening for schedulerConfig...' % rcode)
                if rcode == 0 and self.sched_conf.surveyDuration != 0:
                    self.log.info("run: rx scheduler config survey_duration=%.1f" % self.sched_conf.surveyDuration)
                    break
                else:
                    tf = time.time()
                    if (tf - lasttime) > self.socs_timeout:
                        raise SchedulerTimeoutError("No configuration received from Scheduler!")

            self.config.survey.duration = self.sched_conf.surveyDuration

    def _configure_scheduler_driver(self):
        """Configure and the Scheduler Driver configuration topic.
        """
        if self.no_dds_comm:
            from SALPY_scheduler import scheduler_driverConfigC
            self.sched_driver_conf = scheduler_driverConfigC()
        else:
            self.sched_driver_conf = self.sal.set_subscribe_topic("driverConfig")

            lasttime = time.time()
            while True:
                rcode = self.sal.manager.getNextSample_driverConfig(self.sched_driver_conf)
                self.log.debug('[rcode:%i] - Listening for driverConfig...' % rcode)
                if rcode == 0 and self.sched_driver_conf.timecostTimeMax > 0:
                    # config_dict = self.sal.rtopic_driver_config(self.sal.topic_driverConfig)
                    self.log.info("run: rx scheduler driver config %s" % self.config.sched_driver)
                    break
                else:
                    tf = time.time()
                    if (tf - lasttime) > self.socs_timeout:
                        raise SchedulerTimeoutError("No configuration received from Scheduler!!")

            self.config.sched_driver.coadd_values = bool(self.sched_driver_conf.coaddValues)
            self.config.sched_driver.time_balancing = bool(self.sched_driver_conf.timeBalancing)
            self.config.sched_driver.timecost_time_max = self.sched_driver_conf.timecostTimeMax
            self.config.sched_driver.timecost_time_ref = self.sched_driver_conf.timecostTimeRef
            self.config.sched_driver.timecost_cost_ref = self.sched_driver_conf.timecostCostRef
            self.config.sched_driver.timecost_weight = self.sched_driver_conf.timecostWeight
            self.config.sched_driver.filtercost_weight = self.sched_driver_conf.filtercostWeight
            self.config.sched_driver.propboost_weight = self.sched_driver_conf.propboostWeight
            self.config.sched_driver.night_boundary = self.sched_driver_conf.nightBoundary
            self.config.sched_driver.new_moon_phase_threshold = self.sched_driver_conf.newMoonPhaseThreshold
            self.config.sched_driver.ignore_sky_brightness = bool(self.sched_driver_conf.ignoreSkyBrightness)
            self.config.sched_driver.ignore_airmass = bool(self.sched_driver_conf.ignoreAirmass)
            self.config.sched_driver.ignore_clouds = bool(self.sched_driver_conf.ignoreClouds)
            self.config.sched_driver.ignore_seeing = bool(self.sched_driver_conf.ignoreSeeing)
            self.config.sched_driver.lookahead_window_size = int(self.sched_driver_conf.lookaheadWindowSize)
            self.config.sched_driver.lookahead_bonus_weight = self.sched_driver_conf.lookaheadBonusWeight

    def _configure_observing_site(self):
        """Configure and send the Observing Site configuration topic.
        """
        if self.no_dds_comm:
            from SALPY_scheduler import scheduler_obsSiteConfigC
            self.obs_site_conf = scheduler_obsSiteConfigC()
        else:
            self.obs_site_conf = self.sal.set_subscribe_topic("obsSiteConfig")

            lasttime = time.time()
            while True:
                rcode = self.sal.manager.getNextSample_obsSiteConfig(self.obs_site_conf)
                self.log.debug('[rcode:%i] - Listening for obsSiteConfig...' % rcode)
                if rcode == 0 and self.obs_site_conf.name != "":
                    # config_dict = self.sal.rtopic_driver_config(self.sal.topic_driverConfig)
                    self.log.info("run: rx scheduler site config %s" % self.obs_site_conf.name)
                    break
                else:
                    tf = time.time()
                    if (tf - lasttime) > self.socs_timeout:
                        raise SchedulerTimeoutError("No configuration received from Scheduler!!")

            self.config.observing_site.name = self.obs_site_conf.name
            self.config.observing_site.latitude = self.obs_site_conf.latitude
            self.config.observing_site.longitude = self.obs_site_conf.longitude
            self.config.observing_site.height = self.obs_site_conf.height
            self.config.observing_site.pressure = self.obs_site_conf.pressure
            self.config.observing_site.temperature = self.obs_site_conf.temperature
            self.config.observing_site.relative_humidity = self.obs_site_conf.relativeHumidity

    def _configure_telescope(self):
        """Configure and send the Telescope configuration topic.
        """
        if self.no_dds_comm:
            from SALPY_scheduler import scheduler_telescopeConfigC
            self.tel_conf = scheduler_telescopeConfigC()
        else:
            self.tel_conf = self.sal.set_subscribe_topic("telescopeConfig")

            lasttime = time.time()
            while True:
                rcode = self.sal.manager.getNextSample_telescopeConfig(self.tel_conf)
                self.log.debug('[rcode:%i] - Listening for telescopeConfig...' % rcode)
                if rcode == 0 and self.tel_conf.altitudeMinpos >= 0:
                    # config_dict = self.sal.rtopic_driver_config(self.sal.topic_driverConfig)
                    self.log.info("run: rx scheduler telescope config")
                    break
                else:
                    tf = time.time()
                    if (tf - lasttime) > self.socs_timeout:
                        raise SchedulerTimeoutError("No configuration received from Scheduler!!")

            self.config.observatory.telescope.altitude_minpos = self.tel_conf.altitudeMinpos
            self.config.observatory.telescope.altitude_maxpos = self.tel_conf.altitudeMaxpos
            self.config.observatory.telescope.altitude_maxspeed = self.tel_conf.altitudeMaxspeed
            self.config.observatory.telescope.altitude_accel = self.tel_conf.altitudeAccel
            self.config.observatory.telescope.altitude_decel = self.tel_conf.altitudeDecel
            self.config.observatory.telescope.azimuth_minpos = self.tel_conf.azimuthMinpos
            self.config.observatory.telescope.azimuth_maxpos = self.tel_conf.azimuthMaxpos
            self.config.observatory.telescope.azimuth_maxspeed = self.tel_conf.azimuthMaxspeed
            self.config.observatory.telescope.azimuth_accel = self.tel_conf.azimuthAccel
            self.config.observatory.telescope.azimuth_decel = self.tel_conf.azimuthDecel
            self.config.observatory.telescope.settle_time = self.tel_conf.settleTime

    def _configure_dome(self):
        """Configure and send the dome configuration topic.
        """
        if self.no_dds_comm:
            from SALPY_scheduler import scheduler_domeConfigC
            self.dome_conf = scheduler_domeConfigC()
        else:
            self.dome_conf = self.sal.set_subscribe_topic("domeConfig")

            lasttime = time.time()
            while True:
                rcode = self.sal.manager.getNextSample_domeConfig(self.dome_conf)
                self.log.debug('[rcode:%i] - Listening for domeConfig...' % rcode)
                if rcode == 0 and self.dome_conf.altitudeMaxspeed >= 0:
                    # config_dict = self.sal.rtopic_driver_config(self.sal.topic_driverConfig)
                    self.log.info("run: rx scheduler dome config")
                    break
                else:
                    tf = time.time()
                    if (tf - lasttime) > self.socs_timeout:
                        raise SchedulerTimeoutError("No configuration received from Scheduler!!")

            self.config.observatory.dome.altitude_maxspeed = self.dome_conf.altitudeMaxspeed
            self.config.observatory.dome.altitude_accel = self.dome_conf.altitudeAccel
            self.config.observatory.dome.altitude_decel = self.dome_conf.altitudeDecel
            self.config.observatory.dome.altitude_freerange = self.dome_conf.altitudeFreerange
            self.config.observatory.dome.azimuth_maxspeed = self.dome_conf.azimuthMaxspeed
            self.config.observatory.dome.azimuth_accel = self.dome_conf.azimuthAccel
            self.config.observatory.dome.azimuth_decel = self.dome_conf.azimuthDecel
            self.config.observatory.dome.azimuth_freerange = self.dome_conf.azimuthFreerange
            self.config.observatory.dome.settle_time = self.dome_conf.settleTime

    def _configure_rotator(self):
        """Configure and send the rotator configuration topic.
        """
        if self.no_dds_comm:
            from SALPY_scheduler import scheduler_rotatorConfigC
            self.rot_conf = scheduler_rotatorConfigC()
        else:
            self.rot_conf = self.sal.set_subscribe_topic("rotatorConfig")

            lasttime = time.time()
            while True:
                rcode = self.sal.manager.getNextSample_rotatorConfig(self.rot_conf)
                self.log.debug('[rcode:%i] - Listening for domeConfig...' % rcode)
                if rcode == 0 and self.rot_conf.maxspeed >= 0:
                    # config_dict = self.sal.rtopic_driver_config(self.sal.topic_driverConfig)
                    self.log.info("run: rx scheduler rotator config")
                    break
                else:
                    tf = time.time()
                    if (tf - lasttime) > self.socs_timeout:
                        raise SchedulerTimeoutError("No configuration received from Scheduler!!")

            self.config.observatory.rotator.minpos = self.rot_conf.minpos
            self.config.observatory.rotator.maxpos = self.rot_conf.maxpos
            self.config.observatory.rotator.filter_change_pos = self.rot_conf.filterChangePos
            self.config.observatory.rotator.maxspeed = self.rot_conf.maxspeed
            self.config.observatory.rotator.accel = self.rot_conf.accel
            self.config.observatory.rotator.decel = self.rot_conf.decel
            self.config.observatory.rotator.follow_sky = bool(self.rot_conf.followsky)
            self.config.observatory.rotator.resume_angle = bool(self.rot_conf.resumeAngle)

    def _configure_camera(self):
        """Configure and send the camera configuration topic.
        """
        if self.no_dds_comm:
            from SALPY_scheduler import scheduler_cameraConfigC
            self.cam_conf = scheduler_cameraConfigC()
        else:
            self.cam_conf = self.sal.set_subscribe_topic("cameraConfig")

            lasttime = time.time()
            while True:
                rcode = self.sal.manager.getNextSample_cameraConfig(self.cam_conf)
                self.log.debug('[rcode:%i] - Listening for cameraConfig...' % rcode)
                if rcode == 0 and self.cam_conf.readoutTime > 0:
                    # config_dict = self.sal.rtopic_driver_config(self.sal.topic_driverConfig)
                    self.log.info("run: rx scheduler camera config")
                    break
                else:
                    tf = time.time()
                    if (tf - lasttime) > self.socs_timeout:
                        raise SchedulerTimeoutError("No configuration received from Scheduler!!")

            self.config.observatory.camera.readout_time = self.cam_conf.readoutTime
            self.config.observatory.camera.shutter_time = self.cam_conf.shutterTime
            self.config.observatory.camera.filter_mount_time = self.cam_conf.filterMountTime
            self.config.observatory.camera.filter_change_time = self.cam_conf.filterChangeTime
            self.config.observatory.camera.filter_max_changes_burst_num = int(self.cam_conf.filterMaxChangesBurstNum)
            self.config.observatory.camera.filter_max_changes_burst_time = self.cam_conf.filterMaxChangesBurstTime
            self.config.observatory.camera.filter_max_changes_avg_num = self.cam_conf.filterMaxChangesAvgNum
            self.config.observatory.camera.filter_max_changes_avg_time = self.cam_conf.filterMaxChangesAvgTime
            self.config.observatory.camera.filter_mounted = self.cam_conf.filterMounted.split(',')
            self.config.observatory.camera.filter_pos = self.cam_conf.filterPos
            self.config.observatory.camera.filter_removable = self.cam_conf.filterRemovable.split(',')
            self.config.observatory.camera.filter_unmounted = self.cam_conf.filterUnmounted.split(',')

    def _configure_slew(self):
        """Configure and send the slew configuration topic.
        """
        if self.no_dds_comm:
            from SALPY_scheduler import scheduler_slewConfigC
            self.slew_conf = scheduler_slewConfigC()
        else:
            self.slew_conf = self.sal.set_subscribe_topic("slewConfig")

            lasttime = time.time()
            while True:
                rcode = self.sal.manager.getNextSample_slewConfig(self.slew_conf)
                self.log.debug('[rcode:%i] - Listening for slewConfig...' % rcode)
                if rcode == 0 and self.slew_conf.prereqExposures != "":
                    # config_dict = self.sal.rtopic_driver_config(self.sal.topic_driverConfig)
                    self.log.info("run: rx scheduler slew config")
                    break
                else:
                    tf = time.time()
                    if (tf - lasttime) > self.socs_timeout:
                        raise SchedulerTimeoutError("No configuration received from Scheduler!!")

            self.config.observatory.slew.prereq_domaz = self.slew_conf.prereqDomaz.split(',') if len(
                self.slew_conf.prereqDomaz) > 0 else []
            self.config.observatory.slew.prereq_telalt = self.slew_conf.prereqTelalt.split(',') if len(
                self.slew_conf.prereqTelalt) > 0 else []
            self.config.observatory.slew.prereq_telaz = self.slew_conf.prereqTelaz.split(',') if len(
                self.slew_conf.prereqTelaz) > 0 else []
            self.config.observatory.slew.prereq_telopticsopenloop = self.slew_conf.prereqTelOpticsOpenLoop.split(
                ',') if len(self.slew_conf.prereqTelOpticsOpenLoop) > 0 else []
            self.config.observatory.slew.prereq_telopticsclosedloop = self.slew_conf.prereqTelOpticsClosedLoop.split(
                ',') if len(self.slew_conf.prereqTelOpticsClosedLoop) > 0 else []
            self.config.observatory.slew.prereq_telrot = self.slew_conf.prereqTelRot.split(',') if len(
                self.slew_conf.prereqTelRot) > 0 else []
            self.config.observatory.slew.prereq_filter = self.slew_conf.prereqFilter.split(',') if len(
                self.slew_conf.prereqFilter) > 0 else []
            self.config.observatory.slew.prereq_adc = self.slew_conf.prereqAdc.split(',') if len(
                self.slew_conf.prereqAdc) > 0 else []
            self.config.observatory.slew.prereq_ins_optics = self.slew_conf.prereqInsOptics.split(',') if len(
                self.slew_conf.prereqInsOptics) > 0 else []
            self.config.observatory.slew.prereq_guider_pos = self.slew_conf.prereqGuiderPos.split(',') if len(
                self.slew_conf.prereqGuiderPos) > 0 else []
            self.config.observatory.slew.prereq_guider_adq = self.slew_conf.prereqGuiderAdq.split(',') if len(
                self.slew_conf.prereqGuiderAdq) > 0 else []
            self.config.observatory.slew.prereq_telsettle = self.slew_conf.prereqTelSettle.split(',') if len(
                self.slew_conf.prereqTelSettle) > 0 else []
            self.config.observatory.slew.prereq_domazsettle = self.slew_conf.prereqDomazSettle.split(',') if len(
                self.slew_conf.prereqDomazSettle) > 0 else []
            self.config.observatory.slew.prereq_exposures = self.slew_conf.prereqExposures.split(',') if len(
                self.slew_conf.prereqExposures) > 0 else []
            self.config.observatory.slew.prereq_readout = self.slew_conf.prereqReadout.split(',') if len(
                self.slew_conf.prereqReadout) > 0 else []

    def _configure_optics_loop_corr(self):
        """Configure and send the optics loop correction configuration topic.
        """
        if self.no_dds_comm:
            from SALPY_scheduler import scheduler_opticsLoopCorrConfigC
            self.olc_conf = scheduler_opticsLoopCorrConfigC()
        else:
            self.olc_conf = self.sal.set_subscribe_topic("opticsLoopCorrConfig")

            lasttime = time.time()
            while True:
                rcode = self.sal.manager.getNextSample_opticsLoopCorrConfig(self.olc_conf)
                self.log.debug('[rcode:%i] - Listening for opticsLoopCorrConfig...' % rcode)
                if rcode == 0 and self.olc_conf.telOpticsOlSlope > 0:
                    # config_dict = self.sal.rtopic_driver_config(self.sal.topic_driverConfig)
                    self.log.info("run: rx scheduler optics config")
                    break
                else:
                    tf = time.time()
                    if (tf - lasttime) > self.socs_timeout:
                        raise SchedulerTimeoutError("No configuration received from Scheduler!!")

            self.config.observatory.optics_loop_corr.tel_optics_ol_slope = self.olc_conf.telOpticsOlSlope

            self.config.observatory.optics_loop_corr.tel_optics_cl_alt_limit = self.olc_conf.telOpticsClAltLimit
            self.config.observatory.optics_loop_corr.tel_optics_cl_delay = self.olc_conf.telOpticsClDelay

    def _configure_park(self):
        """Configure and send the park position configuration.
        """
        if self.no_dds_comm:
            from SALPY_scheduler import scheduler_parkConfigC
            self.park_conf = scheduler_parkConfigC()
        else:
            self.park_conf = self.sal.set_subscribe_topic("parkConfig")

            lasttime = time.time()
            while True:
                rcode = self.sal.manager.getNextSample_parkConfig(self.park_conf)
                self.log.debug('[rcode:%i] - Listening for parkConfig...' % rcode)
                if rcode == 0 and self.park_conf.telescopeAltitude > 0:
                    # config_dict = self.sal.rtopic_driver_config(self.sal.topic_driverConfig)
                    self.log.info("run: rx scheduler park config")
                    break
                else:
                    tf = time.time()
                    if (tf - lasttime) > self.socs_timeout:
                        raise SchedulerTimeoutError("No configuration received from Scheduler!")

            self.config.observatory.park.telescope_altitude = self.park_conf.telescopeAltitude
            self.config.observatory.park.telescope_azimuth = self.park_conf.telescopeAzimuth
            self.config.observatory.park.telescope_rotator = self.park_conf.telescopeRotator
            self.config.observatory.park.dome_altitude = self.park_conf.domeAltitude
            self.config.observatory.park.dome_azimuth = self.park_conf.domeAzimuth
            self.config.observatory.park.filter_position = self.park_conf.filterPosition

    def _configure_proposals(self):
        """Publish the general and sequence proposals.
        """
        if self.no_dds_comm:
            from SALPY_scheduler import scheduler_surveyTopologyC
            self.topology_conf = scheduler_surveyTopologyC()
        else:
            self.topology_conf = self.sal.set_subscribe_topic("surveyTopology")

            lasttime = time.time()
            while True:
                rcode = self.sal.manager.getNextSample_surveyTopology(self.topology_conf)
                self.log.debug('[rcode:%i] - Listening for surveyTopology...' % rcode)
                if rcode == 0 and (self.topology_conf.numGeneralProps > 0 or
                                   self.topology_conf.numSequenceProps > 0):
                    # config_dict = self.sal.rtopic_driver_config(self.sal.topic_driverConfig)
                    self.log.info("run: rx scheduler topology config")
                    break
                else:
                    tf = time.time()
                    if (tf - lasttime) > self.socs_timeout:
                        raise SchedulerTimeoutError("No configuration received from Scheduler!")

            self.num_proposals = self.topology_conf.numGeneralProps+self.topology_conf.numSeqProps
            self.survey_topology['general'] = self.topology_conf.generalPropos.split(',')
            self.survey_topology['sequence'] = self.topology_conf.sequencePropos.split(',')

    def configure(self):
        """Configure all publish topics for the configuration communicator.
        """
        self._configure_scheduler()
        self._configure_scheduler_driver()
        self._configure_observing_site()
        self._configure_telescope()
        self._configure_dome()
        self._configure_rotator()
        self._configure_camera()
        self._configure_slew()
        self._configure_optics_loop_corr()
        self._configure_park()
        self._configure_proposals()

    def run(self):
        """Send all of the configuration topics.
        """
        if True:
            raise DeprecationWarning("This method is not used anymore... ")

        self.log.info("Running configuration communication")
        self.sal.put(self.sched_conf)
        self.sal.put(self.sched_driver_conf)
        self.sal.put(self.obs_site_conf)
        self.sal.put(self.tel_conf)
        self.sal.put(self.dome_conf)
        self.sal.put(self.rot_conf)
        self.sal.put(self.cam_conf)
        self.sal.put(self.slew_conf)
        self.sal.put(self.olc_conf)
        self.sal.put(self.park_conf)
        num_proposals = 1
        if self.config.science.general_props.active is not None:
            for general_config in self.config.science.general_props.active:
                general_topic = general_config.set_topic(self.sal.get_topic("generalPropConfig"))
                general_topic.prop_id = num_proposals
                self.sal.put(general_topic)
                num_proposals += 1
        else:
            general_topic = self.sal.get_topic("generalPropConfig")
            general_topic.prop_id = -1
            general_topic.name = "NULL"
            self.sal.put(general_topic)
        if self.config.science.sequence_props.active is not None:
            for sequence_config in self.config.science.sequence_props.active:
                sequence_topic = sequence_config.set_topic(self.sal.get_topic("sequencePropConfig"))
                sequence_topic.prop_id = num_proposals
                self.sal.put(sequence_topic)
                num_proposals += 1
        else:
            sequence_topic = self.sal.get_topic("sequencePropConfig")
            sequence_topic.prop_id = -1
            sequence_topic.name = "NULL"
            self.sal.put(sequence_topic)
        self.log.info("Sent configuration for {} proposals.".format(num_proposals - 1))
