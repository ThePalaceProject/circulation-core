from flask_babel import lazy_gettext as _
from model import CirculationEvent, ExternalIntegration, Session, create, get_one

from .model.configuration import (
    ConfigurationAttributeType,
    ConfigurationGrouping,
    ConfigurationMetadata,
    ConfigurationOption,
)


class LocalAnalyticsProviderConfiguration(ConfigurationGrouping):
    """Contains configuration settings of the analytics provider."""

    # Where to get the 'location' of an analytics event.
    LOCATION_SOURCE = "location_source"

    # The 'location' of an analytics event is the 'neighborhood' of
    # the request's authenticated patron.
    LOCATION_SOURCE_NEIGHBORHOOD = "neighborhood"

    # Analytics events have no 'location'.
    LOCATION_SOURCE_DISABLED = ""

    location_source = ConfigurationMetadata(
        key=LOCATION_SOURCE,
        label=_("Geographic location of events"),
        description=_(
            "Local analytics events may have a geographic location associated with them. "
            "How should the location be determined?"
            "<p>Note: to use the patron's neighborhood as the event location, "
            "you must also tell your patron authentication mechanism how "
            "to <i>gather</i> a patron's neighborhood information.</p>"
        ),
        type=ConfigurationAttributeType.SELECT,
        required=True,
        default=LOCATION_SOURCE_DISABLED,
        options=[
            ConfigurationOption(LOCATION_SOURCE_DISABLED, _("Disable this feature.")),
            ConfigurationOption(
                LOCATION_SOURCE_NEIGHBORHOOD,
                _("Use the patron's neighborhood as the event location."),
            ),
        ],
    )


class LocalAnalyticsProvider(object):
    """Base analytics provider."""

    NAME = _("Local Analytics")

    DESCRIPTION = _("Store analytics events in the 'circulationevents' database table.")

    # A given site can only have one analytics provider.
    CARDINALITY = 1

    SETTINGS = LocalAnalyticsProviderConfiguration.to_settings()

    def __init__(self, integration, library=None):
        self.integration_id = integration.id
        self.location_source = (
            integration.setting(
                LocalAnalyticsProviderConfiguration.LOCATION_SOURCE
            ).value
            or LocalAnalyticsProviderConfiguration.LOCATION_SOURCE_DISABLED
        )
        if library:
            self.library_id = library.id
        else:
            self.library_id = None

    def _collect_event(
        self,
        db,
        library,
        license_pool,
        event_type,
        time,
        neighborhood,
        old_value=None,
        new_value=None,
    ):
        """Log the event using the appropriate for the specific provider's mechanism.

        NOTE: This method may be overridden in child classes.

        :param db: Database session
        :type db: sqlalchemy.orm.session.Session

        :param library: Library associated with the event
        :type library: core.model.library.Library

        :param license_pool: License pool associated with the event
        :type license_pool: core.model.licensing.LicensePool

        :param event_type: Type of the event
        :type event_type: str

        :param time: Event's timestamp
        :type time: datetime.datetime

        :param neighborhood: Geographic location of the event
        :type neighborhood: str

        :param old_value: Old value of the metric changed by the event
        :type old_value: Any

        :param new_value: New value of the metric changed by the event
        :type new_value: Any

        :return: 2-tuple containing a CirculationEvent object and a boolean value indicating
            whether it's been created or was loaded from the database
        :rtype: Tuple[CirculationEvent, bool]
        """
        return CirculationEvent.log(
            db,
            license_pool,
            event_type,
            old_value,
            new_value,
            start=time,
            library=library,
            location=neighborhood,
        )

    def collect_event(
        self,
        library,
        license_pool,
        event_type,
        time,
        old_value=None,
        new_value=None,
        **kwargs
    ):
        """Log the event using the appropriate for the specific provider's mechanism.

        :param library: Library associated with the event
        :type library: core.model.library.Library

        :param license_pool: License pool associated with the event
        :type Optional[license_pool]: core.model.licensing.LicensePool

        :param event_type: Type of the event
        :type event_type: str

        :param time: Event's timestamp
        :type time: datetime.datetime

        :param old_value: Old value of the metric changed by the event
        :type old_value: Any

        :param new_value: New value of the metric changed by the event
        :type new_value: Any

        :param kwargs:

        :return: 2-tuple containing a CirculationEvent object and a boolean value indicating
            whether it's been created or was loaded from the database
        :rtype: Tuple[CirculationEvent, bool]
        """
        if not library and not license_pool:
            raise ValueError("Either library or license_pool must be provided.")
        if library:
            db = Session.object_session(library)
        else:
            db = Session.object_session(license_pool)
        if library and self.library_id and library.id != self.library_id:
            return

        neighborhood = None
        if (
            self.location_source
            == LocalAnalyticsProviderConfiguration.LOCATION_SOURCE_NEIGHBORHOOD
        ):
            neighborhood = kwargs.pop("neighborhood", None)

        return self._collect_event(
            db,
            library,
            license_pool,
            event_type,
            time,
            neighborhood,
            old_value,
            new_value,
        )

    @classmethod
    def initialize(cls, _db):
        """Find or create a local analytics service."""

        # If a local analytics service already exists, return it.
        local_analytics = get_one(
            _db,
            ExternalIntegration,
            protocol=cls.__module__,
            goal=ExternalIntegration.ANALYTICS_GOAL,
        )

        # If a local analytics service already exists, don't create a
        # default one. Otherwise, create it with default name of
        # "Local Analytics".
        if not local_analytics:
            local_analytics, ignore = create(
                _db,
                ExternalIntegration,
                protocol=cls.__module__,
                goal=ExternalIntegration.ANALYTICS_GOAL,
                name=str(cls.NAME),
            )
        return local_analytics


Provider = LocalAnalyticsProvider
