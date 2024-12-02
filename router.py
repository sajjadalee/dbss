from django.conf import settings


class TableRouter:
    """
    A database router that routes read operations to table_a_replica and write operations to table_a.
    """

    def db_for_read(self, model, **hints):
        """
        Routes read operations to table_a_replica.
        """
        # Checking for the specific model where you want read redirection
        if settings.DB_ROUTING == 1:
            if (
                model._meta.db_table == 'contracts_subscriptionpackage'
                or model._meta.db_table == 'contracts_subscriptionpackage_new'
            ):
                model._meta.db_table = 'combinedView'
                # Clear the internal state after modifying the table name
                model._meta._expire_cache()

            if (
                model._meta.db_table == 'contracts_subscriptionpackage_services'
                or model._meta.db_table == 'contracts_subscriptionpackage_services_new'
            ):
                model._meta.db_table = 'view_contracts_subscriptionpackage_services'
                # Clear the internal state after modifying the table name
                model._meta._expire_cache()

            if (
                model._meta.db_table == 'contracts_subscriptionpackageservice'
                or model._meta.db_table == 'contracts_subscriptionpackageservice_new'
            ):
                model._meta.db_table = 'view_contracts_subscriptionpackageservice'
                # Clear the internal state after modifying the table name
                model._meta._expire_cache()

            if (
                model._meta.db_table == 'contracts_subscriptionpackageserviceattribute'
                or model._meta.db_table
                == 'contracts_subscriptionpackageserviceattribute_new'
            ):
                model._meta.db_table = (
                    'view_contracts_subscriptionpackageserviceattribute'
                )
                # Clear the internal state after modifying the table name
                model._meta._expire_cache()
            return None

        return None  # Allow default behavior for everything else

    def db_for_write(self, model, **hints):
        """
        Routes write operations to table_a (default table).
        """
        if settings.DB_ROUTING == 1:
            # Ensuring writes go to the original table
            if (
                model._meta.db_table == 'combinedView'
                or model._meta.db_table == 'contracts_subscriptionpackage'
            ):
                model._meta.db_table = 'contracts_subscriptionpackage_new'
                # Clear the internal state after modifying the table name
                model._meta._expire_cache()
            if (
                model._meta.db_table == 'view_contracts_subscriptionpackage_services'
                or model._meta.db_table == 'contracts_subscriptionpackage_services'
            ):
                model._meta.db_table = 'contracts_subscriptionpackage_services_new'
                # Clear the internal state after modifying the table name
                model._meta._expire_cache()
            if (
                model._meta.db_table == 'view_contracts_subscriptionpackageservice'
                or model._meta.db_table == 'contracts_subscriptionpackageservice'
            ):
                model._meta.db_table = 'contracts_subscriptionpackageservice_new'
                # Clear the internal state after modifying the table name
                model._meta._expire_cache()

            if (
                model._meta.db_table
                == 'view_contracts_subscriptionpackageserviceattribute'
                or model._meta.db_table
                == 'contracts_subscriptionpackageserviceattribute'
            ):
                model._meta.db_table = (
                    'contracts_subscriptionpackageserviceattribute_new'
                )
                # Clear the internal state after modifying the table name
                model._meta._expire_cache()
            return None

        return None  # Allow default behavior for everything else

    def allow_relation(self, obj1, obj2, **hints):
        """
        Allow relations between models in both tables.
        """
        return True

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        """
        Ensure migrations are done only on the default table.
        """
        return True
