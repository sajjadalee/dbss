from django.db import models, connection, router
from django.db.models import QuerySet, Q, F, Lookup
from django.conf import settings


class SubscriptionPackageQuerySet(models.QuerySet):
    def _rewrite_table_name(self):
        """Rewrite the table names dynamically in the query alias map."""
        if not hasattr(self.query, "alias_map"):
            return  # Skip if alias_map is not available (e.g., raw queries)

        for alias, table in self.query.alias_map.items():
            if table.table_name == 'contracts_subscriptionpackage_services':
                table.table_name = 'view_contracts_subscriptionpackage_services'
            if table.table_name == 'contracts_subscriptionpackageservice':
                table.table_name = 'view_contracts_subscriptionpackageservice'
        # Rewrite explicit table names in WHERE conditions (if required)
        if hasattr(self.query, "where"):
            self._rewrite_where(self.query.where)

    def _rewrite_where(self, where_clause):
        """Recursively rewrite table names in the WHERE clause."""
        if hasattr(where_clause, "children"):
            for child in where_clause.children:
                if hasattr(child, "lhs") and hasattr(child.lhs, "target"):
                    # Check if the child is a reference to a table/column
                    table_name = child.lhs.target.model._meta.db_table
                    if table_name == 'contracts_subscriptionpackage_services':
                        child.lhs.target.model._meta.db_table = (
                            'view_contracts_subscriptionpackage_services'
                        )
                    elif table_name == 'contracts_subscriptionpackageservice':
                        child.lhs.target.model._meta.db_table = (
                            'view_contracts_subscriptionpackageservice'
                        )

                # Handle nested conditions
                if hasattr(child, "children"):
                    self._rewrite_where(child)

    def _fetch_all(self):
        """Ensure table names are rewritten before fetching results."""
        self._result_cache = None  # Clear the cache
        self.query.clear_ordering(
            force_empty=True
        )  # Clear ordering, which might rely on alias_map
        self._rewrite_table_name()
        super()._fetch_all()

    def _as_sql(self, *args, **kwargs):
        # Call the original method to get the SQL query
        sql, params = super()._as_sql(*args, **kwargs)
        # Replace the table name in the SQL query
        sql = sql.replace('contracts_subscriptionpackage', 'combinedView')
        sql = sql.replace('contracts_subscriptionpackage_services', 'combinedView')

        return sql, params

    def filter(self, *args, **kwargs):

        # Ensure that the db router is invoked properly
        from django.db import router

        router.db_for_read(self.model)  # Force router evaluation
        # Replace `contracts_subscriptionpackage` with `combinedView` in the filter conditions
        filters = {}

        for key, value in kwargs.items():
            # Modify the FK filters to replace the incorrect table reference
            new_key = key.replace('contracts_subscriptionpackage', 'combinedView')
            filters[new_key] = value
            # Modify select related parts if select is used
            if 'select' in kwargs:
                select_fields = kwargs.get('select', {})
                new_select_fields = {}
                for field, value in select_fields.items():
                    # Replace the table reference in the select fields
                    new_field = field.replace(
                        'contracts_subscriptionpackage', 'combinedView'
                    )
                    new_select_fields[new_field] = value
                kwargs['select'] = new_select_fields
            # Handle the case where related fields might still reference the old table
            if 'select_related' in kwargs:
                select_related_fields = kwargs['select_related']
                # Force router evaluation for all related models
                for related_field in select_related_fields:
                    related_model = self.model._meta.get_field(
                        related_field
                    ).related_model
                    router.db_for_read(related_model)

            if 'prefetch_related' in kwargs:
                prefetch_related_fields = kwargs['prefetch_related']
                # Force router evaluation for all related models
                for related_field in prefetch_related_fields:
                    related_model = self.model._meta.get_field(
                        related_field
                    ).related_model
                    router.db_for_read(related_model)

        return super().filter(*args, **filters)


class SubscriptionPackageManager(models.Manager):
    def active(self, include_suspended=False, *args, **kwargs):
        result = self.filter(
            Q(activated_at__lte=datetime_now()),
            Q(deactivated_at__isnull=True) | Q(deactivated_at__gt=datetime_now()),
            *args,
            **kwargs,
        )
        if not include_suspended:
            return result.exclude(suspended_at__isnull=False)
        return result

    def inactive(self, *args, **kwargs):
        return self.filter(deactivated_at__lte=datetime_now(), *args, **kwargs)

    def active_package_codes(self, include_suspended=False, *args, **kwargs):
        return (
            self.active(include_suspended=include_suspended)
            .select_related("package")
            .values_list("package__code", flat=True)
        )


class SubscriptionPackageManagerNew(SubscriptionPackageManager):
    def get_queryset(self):
        # Ensure that the router is invoked here for the base model
        router.db_for_read(self.model)
        # Return a custom queryset that operates on combinedView
        return SubscriptionPackageQuerySet(self.model, using=self._db)




class SubscriptionPackage(DWHDumpable):
    """
    Replaces Subscription.packages many to many relationship with
    join table which provides information about past packages,
    their activation and deactivation times.

    Services attributes are stored in a proper relational database
    form.

    Non-Recurrent packages are stored with exactly same activation
    and deactivation times.

    `activated_by`
        The memo id of which issued the change. This can
        be null if change was done via unknown methods.

    `deactivated_by`
        The memo id of which issued the change.
        Note that this can be NULL for deactivated services
        if the deactivation was done outside Memos.

    `grace_period`
        Number of days as grace period before the package gets
        deactivated.

    `grace_period_start_at`
        The start date of the grace period for the package.

    `fee`
        The fee for the package at the time of activation.

    `billing_account`
        Foreign Key to billing account of the subscription which is
        related to the package activation. Information is used by
        BSSAPI.

    `product_incentives_counter`
        Number of instance where package has been re-purchased using the
        accumulation volume mode.

    `suspended_at`
        Possible point of time the package was suspended.

    `keep_till`
        Data to be persisted till the value before removing/truncating/purging the partition.
    """

    objects: SubscriptionPackageManager
    if settings.DB_ROUTING == 1:
        objects = SubscriptionPackageManagerNew()
    else:
        objects = SubscriptionPackageManager()

    id = models.BigAutoField(primary_key=True)
    subscription = models.ForeignKey(
        'contracts.Subscription',
        db_index=True,
        related_name='xref_packages',
        on_delete=models.CASCADE,
    )
    package = models.ForeignKey(
        'products.Package', db_index=True, on_delete=models.CASCADE
    )
    activated_at = models.DateTimeField(default=datetime_now)
    deactivated_at = models.DateTimeField(default=None, null=True)
    services = models.ManyToManyField(
        'contracts.SubscriptionPackageService',
        through='contracts.SubscriptionPackageLinkService',
    )
    activated_by = models.BigIntegerField(null=True)
    deactivated_by = models.BigIntegerField(null=True)
    grace_period = models.IntegerField(default=None, null=True)
    grace_period_start_at = models.DateTimeField(default=None, null=True)
    fee = models.DecimalField(decimal_places=2, max_digits=32, default=0.0)
    billing_account = models.ForeignKey(
        'contracts.BillingAccount', null=True, on_delete=models.DO_NOTHING
    )
    package_offer = models.ForeignKey(
        'offers.PackageOffer', null=True, on_delete=models.DO_NOTHING
    )
    product_incentives_counter = models.IntegerField(default=0)
    suspended_at = models.DateTimeField(default=None, null=True, db_index=True)
    keep_till = models.IntegerField(default=999)

    def save(self, *args, **kwargs):
        if settings.DB_ROUTING == 1:
            if self.pk:
                # If args are provided, use the fields in args; otherwise, fallback to all model fields
                if args:
                    fields = args[0]  # Use the provided list in args
                else:
                    fields = [
                        field.name for field in self._meta.get_fields()
                    ]  # Fallback to all fields

                # Iterate through the fields
                fields_to_update = {}
                for field_name in fields:
                    # Check if the field exists on the model
                    if hasattr(self, field_name):
                        field_value = getattr(self, field_name)

                        # Get the actual field object by field name
                        field = self._meta.get_field(field_name)

                        # Skip ManyToManyFields
                        if isinstance(field, models.ManyToManyField) or isinstance(
                            field, models.ManyToOneRel
                        ):
                            continue  # Skip processing ManyToMany fields

                        # Check if the field is a ForeignKey
                        if isinstance(field, models.ForeignKey):
                            # Get the ForeignKey ID instead of the object
                            fk_value = getattr(self, field_name + "_id")
                            fields_to_update[field_name + "_id"] = fk_value
                        else:
                            # For non-ForeignKey fields, store the value
                            fields_to_update[field_name] = field_value

                if fields_to_update:
                    set_clause = ", ".join(
                        [f"{field} = %s" for field in fields_to_update.keys()]
                    )
                    values = list(fields_to_update.values())
                    values.append(
                        self.id
                    )  # Add the id to the values list for the WHERE clause

                    if self.keep_till < 1000:
                        old_instance = SubscriptionPackage.objects.filter(
                            id=self.id
                        ).first()
                        if old_instance:
                            sql_query = f"UPDATE contracts_subscriptionpackage SET {set_clause} WHERE id = %s"
                    else:
                        new_instance = SubscriptionPackageNew.objects.filter(
                            id=self.id
                        ).first()
                        if new_instance:
                            sql_query = f"UPDATE contracts_subscriptionpackage_new SET {set_clause} WHERE id = %s"
                    with connection.cursor() as cursor:
                        cursor.execute(sql_query, values)

            else:
                try:
                    super().save(*args, **kwargs)

                except Exception as e:
                    logging.error(f"Error during saving the new instance: {str(e)}")
                    raise
        else:
            try:
                super().save(*args, **kwargs)

            except Exception as e:
                logging.error(f"Error during saving the old instance: {str(e)}")
                raise

    def deactivate(
        self,
        deactivated_by: Optional[Memo] = None,
        deactivated_at: Optional[datetime] = None,
        package: Optional[Package] = None,
        keep_till: Optional[int] = None,
    ):
        """
        Set deactivation time and cascade down to related models.

        :param deactivated_by: The possible memo which triggered the
            change.
        :param deactivated_at: Set deactivation time explicitly.
            Otherwise use memos executed_at time or fall back to
            current time.
        :param package: Package to be deactivated, also used for calculation of keep_till partition value
        :param keep_till: The value for retention of partition
        """
        self.deactivated_at = _action_time(deactivated_by, deactivated_at)
        self.deactivated_by = deactivated_by.id if deactivated_by else None
        if self.keep_till is None:
            if keep_till is None:
                self.keep_till = self.subscription.calculate_keep_till(
                    package, self.deactivated_at
                )
            else:
                self.keep_till = keep_till
        self.suspended_at = None
        for service in self.services.all():
            service.deactivate(
                deactivated_by,
                deactivated_at=self.deactivated_at,
                package=package,
                keep_till=self.keep_till,
            )
        self.save()

    def suspend(self, deactivated_by: Optional[Memo] = None):
        """
        Set the package as suspended by setting the suspension time.

        :param deactivated_by: The possible memo which triggered the
            change.
        """
        self.suspended_at = _action_time(deactivated_by)
        self.save()

    def resume(self):
        """
        Remove the package suspension by setting suspension time as
        None.
        """
        self.suspended_at = None
        self.save()

    def __str__(self):
        return '{},activated_at={},deactivated_at={}'.format(
            self.package.code, self.activated_at, self.deactivated_at
        )

    class Meta:
        indexes = [
            models.Index(fields=['-activated_at']),
            models.Index(fields=['-deactivated_at']),
        ]

def replace_table_name(old_name):
    """
    Replace the old table name with the new view table name.
    This function is used for dynamic table name replacements.
    """
    replacements = {
        "contracts_subscriptionpackage_services": "view_contracts_subscriptionpackage_services",
        "contracts_subscriptionpackageservice": "view_contracts_subscriptionpackageservice",
        "contracts_subscriptionpackage_services_new": "view_contracts_subscriptionpackage_services",
        "contracts_subscriptionpackageservice_new": "view_contracts_subscriptionpackageservice",
    }

    return replacements.get(
        old_name, old_name
    )  # Return the new name or the original name if no replacement found.

class PackageServiceQuerySet(models.QuerySet):
    def _rewrite_alias_map(self):
        """
        Update alias_map table names dynamically to use view tables.
        """
        for alias, table in self.query.alias_map.items():
            original_name = table.table_name
            table.table_name = replace_table_name(table.table_name)
            print(f"Rewriting Alias: {alias} -> {original_name} to {table.table_name}")

    def _rewrite_where(self, where_clause):
        """
        Recursively rewrite table names in the WHERE clause.
        """
        if hasattr(where_clause, 'children'):
            for child in where_clause.children:
                self._rewrite_where(child)
                if hasattr(child, 'lhs') and hasattr(child.lhs, 'target'):
                    child.lhs.target.model._meta.db_table = replace_table_name(
                        child.lhs.target.model._meta.db_table
                    )
                if hasattr(child, 'rhs') and isinstance(child.rhs, str):
                    child.rhs = replace_table_name(child.rhs)

        where_clause = self._rewrite_where_clause(where_clause)
        return where_clause

    def _rewrite_where_clause(self, where_clause):
        """
        Handle table replacements in the WHERE clause expression.
        """
        if isinstance(where_clause, Q):
            for child in where_clause.children:
                self._rewrite_where(child)

        # Replace table names in the WHERE clause expression
        if isinstance(where_clause, Lookup):
            self._rewrite_lookup(where_clause)

        return where_clause

    def _rewrite_lookup(self, lookup):
        """
        Handle the replacement of table names in a Lookup expression.
        """
        if hasattr(lookup.lhs, 'model'):
            # Rewrite the table name in the left-hand side model
            lookup.lhs.model._meta.db_table = replace_table_name(
                lookup.lhs.model._meta.db_table
            )

        if isinstance(lookup.rhs, str):
            # If the right-hand side is a string, replace the table name
            lookup.rhs = replace_table_name(lookup.rhs)

        return lookup

    def _rewrite_expression(self, expression):
        """
        Rewrite the table names in complex expressions like F() expressions.
        """
        if isinstance(expression, F):
            if hasattr(expression, 'target'):
                expression.target.model._meta.db_table = replace_table_name(
                    expression.target.model._meta.db_table
                )

        return expression

    def _fetch_all(self):
        """
        Ensure table names are rewritten before fetching query results.
        """
        if not self._result_cache:
            print(f"query before rewrite : {self.query}")
            self._rewrite_alias_map()
            self._rewrite_where(self.query.where)
            print(f"query after rewrite : {self.query}")

        super()._fetch_all()

    def filter(self, *args, **kwargs):
        # Log the original query parameters
        print(f"Original filter kwargs: {kwargs}")

        # Replace table names in the filter kwargs to ensure the correct table names are used
        new_kwargs = {}

        # Loop through the kwargs to replace table names in the keys
        for key, value in kwargs.items():
            # If the key contains a table reference (e.g., 'contracts_subscriptionpackage_services')
            new_key = replace_table_name(
                key
            )  # Replace the table name in the filter key
            new_kwargs[
                new_key
            ] = value  # Add the modified key and value to the new kwargs

        # Log the updated query parameters
        print(f"Updated filter kwargs: {new_kwargs}")

        # Call the parent `filter` method with the updated kwargs
        return super().filter(*args, **new_kwargs)

    def _rewrite_filters(self, kwargs):
        """
        Update filter arguments dynamically to replace table names.
        """
        updated_kwargs = {}

        for field, value in kwargs.items():
            if isinstance(value, list):
                # Handle cases where the filter value is a list (e.g., 'subscriptionpackage__in')
                updated_value = [self._replace_table_names(val) for val in value]
            else:
                # Handle other cases of filter values
                updated_value = self._replace_table_names(value)

            updated_kwargs[field] = updated_value
        return updated_kwargs

    def _replace_table_names(self, value):
        """
        Replace table names in the filter value (could be Q, Lookup, or string).
        """
        if isinstance(value, Q):
            # If it's a Q object, replace table names recursively in its children
            value = value.children
            value = [self._replace_table_names(val) for val in value]
        elif isinstance(value, Lookup):
            # Handle replacements in Lookup objects
            self._rewrite_lookup(value)
        elif isinstance(value, str):
            # If it's a string, replace any table names in it
            value = replace_table_name(value)
        return value

    def get_prefetch_queryset(self, instances, related_objects, queryset):
        """
        Ensure prefetch queries also use the rewritten table names.
        """
        queryset._rewrite_alias_map()
        queryset._rewrite_where(queryset.query.where)
        return super().get_prefetch_queryset(instances, related_objects, queryset)


class PackageServiceManager(models.Manager):
    def active(self, include_suspended=False, *args, **kwargs):
        result = self.filter(
            Q(activated_at__lte=datetime_now()),
            Q(deactivated_at__isnull=True) | Q(deactivated_at__gt=datetime_now()),
            *args,
            **kwargs,
        )
        if not include_suspended:
            return result.exclude(suspended_at__isnull=False)
        return result

    def inactive(self, *args, **kwargs):
        return self.filter(deactivated_at__lte=datetime_now(), *args, **kwargs)

    def active_package_codes(self, include_suspended=False, *args, **kwargs):
        return (
            self.active(include_suspended=include_suspended)
            .select_related("package")
            .values_list("package__code", flat=True)
        )


class PackageServiceManagerNew(PackageServiceManager):
    def get_queryset(self):
        # Ensure that the router is invoked here for the base model
        router.db_for_read(self.model)
        # Return a custom queryset that operates on combinedView
        queryset = PackageServiceQuerySet(self.model, using=self._db)
        # Return the modified queryset
        # queryset._rewrite_table_name()
        return queryset


class SubscriptionPackageService(DWHDumpable):
    """
    `activated_by`
        The memo id which issued the change. This can
        be null if change was done via unknown methods OR without
        memo being present. One example of such use case is
        updates coming from CommonAPI (Zattoo).

        Note: Those should be adapted to use Memo as well.

    `deactivated_by`
        The memo id which issued the change.
        Note that this can be NULL for deactivated services
        if the deactivation was done outside Memos.

    `keep_till`
        Data to be kept alive till the value before removing/truncating/purging the partition.
    """

    objects: PackageServiceManager
    if settings.DB_ROUTING == 1:
        objects = PackageServiceManagerNew()
    else:
        objects = PackageServiceManager()

    id = models.BigAutoField(primary_key=True)
    subscription = models.ForeignKey(
        'contracts.Subscription', db_index=True, on_delete=models.CASCADE
    )
    service = models.ForeignKey(
        'products.Service', db_index=True, on_delete=models.CASCADE
    )
    activated_at = models.DateTimeField(default=datetime_now)
    deactivated_at = models.DateTimeField(default=None, null=True)
    activated_by = models.BigIntegerField(null=True)
    deactivated_by = models.BigIntegerField(null=True)
    suspended_at = models.DateTimeField(default=None, null=True)
    keep_till = models.IntegerField(default=999)

    def save(self, *args, **kwargs):
        if settings.DB_ROUTING == 1:
            if self.pk:
                # If args are provided, use the fields in args; otherwise, fallback to all model fields
                if args:
                    fields = args[0]  # Use the provided list in args
                else:
                    fields = [
                        field.name for field in self._meta.get_fields()
                    ]  # Fallback to all fields

                # Iterate through the fields
                fields_to_update = {}
                for field_name in fields:
                    # Check if the field exists on the model
                    if hasattr(self, field_name):
                        field_value = getattr(self, field_name)

                        # Get the actual field object by field name
                        field = self._meta.get_field(field_name)

                        # Skip ManyToManyFields
                        if isinstance(field, models.ManyToManyField) or isinstance(
                            field, models.ManyToOneRel
                        ):
                            continue  # Skip processing ManyToMany fields

                        # Check if the field is a ForeignKey
                        if isinstance(field, models.ForeignKey):
                            # Get the ForeignKey ID instead of the object
                            fk_value = getattr(self, field_name + "_id")
                            fields_to_update[field_name + "_id"] = fk_value
                        else:
                            # For non-ForeignKey fields, store the value
                            fields_to_update[field_name] = field_value
                # If there are fields to update, build and execute the SQL query
                if fields_to_update:
                    set_clause = ", ".join(
                        [f"{field} = %s" for field in fields_to_update.keys()]
                    )
                    values = list(fields_to_update.values())
                    values.append(
                        self.id
                    )  # Add the id to the values list for the WHERE clause

                    if self.keep_till < 1000:
                        old_instance = SubscriptionPackageService.objects.filter(
                            id=self.id
                        ).first()
                        if old_instance:
                            sql_query = f"UPDATE contracts_subscriptionpackageservice SET {set_clause} WHERE id = %s"
                    else:
                        new_instance = SubscriptionPackageServiceNew.objects.filter(
                            id=self.id
                        ).first()
                        if new_instance:
                            sql_query = f"UPDATE contracts_subscriptionpackageservice_new SET {set_clause} WHERE id = %s"
                    with connection.cursor() as cursor:
                        cursor.execute(sql_query, values)

            else:
                try:
                    self._meta.db_table = 'contracts_subscriptionpackageservice_new'
                    super().save(*args, **kwargs)

                except Exception as e:
                    logging.error(f"Error during saving the new instance: {str(e)}")
                    raise
        else:
            try:
                super().save(*args, **kwargs)

            except Exception as e:
                logging.error(f"Error during saving the old instance: {str(e)}")
                raise

    def deactivate(
        self,
        deactivated_by: Optional[Memo] = None,
        deactivated_at: Optional[datetime] = None,
        package: Optional[Package] = None,
        keep_till: Optional[int] = None,
    ):
        """
        Set deactivation time.

        :param deactivated_by: The possible memo which triggered the
            change.
        :param deactivated_at: Set deactivation time explicitly.
            Otherwise use memos executed_at time or fall back to
            current time.
        :param package: Package to be deactivated, also used for calculation of keep_till partition value
        :param keep_till: The value for retention of partition
        """
        self.deactivated_at = _action_time(deactivated_by, deactivated_at)
        self.deactivated_by = deactivated_by.id if deactivated_by else None
        if not keep_till:
            self.keep_till = self.subscription.calculate_keep_till(
                package, self.deactivated_at
            )
        else:
            self.keep_till = keep_till
        self.save()

    def save_new_attributes(self, attrs):
        """
        .. NOTE:: Quick solution, do not use if possible!

        :param dict attrs:
            key must be a attribute code
            value must be attribute value
        """
        switched_at = datetime_now()
        self.deactivate(deactivated_at=switched_at)

        sps = SubscriptionPackageService.objects.create(
            subscription=self.subscription,
            service=self.service,
            activated_at=switched_at,
        )
        # add new subscription package service to all subscription packages
        for spkg in self.subscriptionpackage_set.all():
            spkg.services.add(sps)
        # create new attributes
        for attr_code, value in attrs.items():
            if not value:
                continue
            attr = pc.ServiceAttribute.objects.get(code=attr_code)
            SubscriptionPackageServiceAttribute.objects.create(
                subscription_package_service=sps, attribute=attr, value=value
            )

    def suspend(self, deactivated_by: Optional[Memo] = None):
        """
        Set the package as suspended by setting the suspension time.

        :param deactivated_by: The possible memo which triggered the
            change.
        """
        self.suspended_at = _action_time(deactivated_by)
        self.save()

    def resume(self):
        """
        Remove the package suspension by setting suspension time as
        None.
        """
        self.suspended_at = None
        self.save()

    def __str__(self):
        return 'service={},activated_at={},deactivated_at={}'.format(
            self.service.code, self.activated_at, self.deactivated_at
        )


class SubscriptionPackageLinkServiceQuerySet(models.QuerySet):
    def _rewrite_table_name(self):
        """Rewrite the table names dynamically in the query alias map."""
        if not hasattr(self.query, "alias_map"):
            return  # Skip if alias_map is not available (e.g., raw queries)

        for alias, table in self.query.alias_map.items():
            if table.table_name == 'contracts_subscriptionpackage_services':
                table.table_name = 'view_contracts_subscriptionpackage_services'
            if table.table_name == 'contracts_subscriptionpackageservice':
                table.table_name = 'view_contracts_subscriptionpackageservice'
        # Rewrite explicit table names in WHERE conditions (if required)
        if hasattr(self.query, "where"):
            self._rewrite_where(self.query.where)

    def _rewrite_where(self, where_clause):
        """Recursively rewrite table names in the WHERE clause."""
        if hasattr(where_clause, "children"):
            for child in where_clause.children:
                if hasattr(child, "lhs") and hasattr(child.lhs, "target"):
                    # Check if the child is a reference to a table/column
                    table_name = child.lhs.target.model._meta.db_table
                    if table_name == 'contracts_subscriptionpackage_services':
                        child.lhs.target.model._meta.db_table = (
                            'view_contracts_subscriptionpackage_services'
                        )
                    elif table_name == 'contracts_subscriptionpackageservice':
                        child.lhs.target.model._meta.db_table = (
                            'view_contracts_subscriptionpackageservice'
                        )

                # Handle nested conditions
                if hasattr(child, "children"):
                    self._rewrite_where(child)

    def _as_sql(self, *args, **kwargs):
        # Call the original method to get the SQL query
        sql, params = super()._as_sql(*args, **kwargs)
        # Replace the table name in the SQL query
        sql = sql.replace(
            'contracts_subscriptionpackageservice',
            'view_contracts_subscriptionpackageservice',
        )
        sql = sql.replace(
            'contracts_subscriptionpackage_services',
            'view_contracts_subscriptionpackage_services',
        )

        return sql, params

    def _fetch_all(self):
        """Ensure table names are rewritten before fetching results."""
        self._result_cache = None  # Clear the cache
        self.query.clear_ordering(
            force_empty=True
        )  # Clear ordering, which might rely on alias_map
        self._rewrite_table_name()
        super()._fetch_all()

    def filter(self, *args, **kwargs):
        
        router.db_for_read(self.model)  # Force router evaluation
        # Replace `contracts_subscriptionpackage` with `combinedView` in the filter conditions
        filters = {}

        for key, value in kwargs.items():
            # Modify the FK filters to replace the incorrect table reference
            new_key = key.replace(
                'contracts_subscriptionpackage_services',
                'view_contracts_subscriptionpackage_services',
            )
            filters[new_key] = value
            # Modify select related parts if select is used
            if 'select' in kwargs:
                select_fields = kwargs.get('select', {})
                new_select_fields = {}
                for field, value in select_fields.items():
                    # Replace the table reference in the select fields
                    new_field = field.replace(
                        'contracts_subscriptionpackage_services',
                        'view_contracts_subscriptionpackage_services',
                    )
                    new_select_fields[new_field] = value
                kwargs['select'] = new_select_fields
            # Handle the case where related fields might still reference the old table
            if 'select_related' in kwargs:
                select_related_fields = kwargs['select_related']
                # Force router evaluation for all related models
                for related_field in select_related_fields:
                    related_model = self.model._meta.get_field(
                        related_field
                    ).related_model
                    router.db_for_read(related_model)

            if 'prefetch_related' in kwargs:
                prefetch_related_fields = kwargs['prefetch_related']
                # Force router evaluation for all related models
                for related_field in prefetch_related_fields:
                    related_model = self.model._meta.get_field(
                        related_field
                    ).related_model
                    router.db_for_read(related_model)

        return super().filter(*args, **filters)

    def get_prefetch_queryset(self, related_objects, instance):
        """Override the method to rewrite table names for the prefetch related fields."""
        queryset = super().get_prefetch_queryset(related_objects, instance)
        # Rewrite the table names in the related query
        queryset._rewrite_table_name()
        return queryset


class SubscriptionPackageLinkServiceManager(models.Manager):
    def get_queryset(self):
        # Ensure that the router is invoked here for the base model
        router.db_for_read(self.model)
        # Return a custom queryset that operates on combinedView
        return SubscriptionPackageLinkServiceQuerySet(self.model, using=self._db)


class SubscriptionPackageLinkService(models.Model):
    subscriptionpackage = models.ForeignKey(
        SubscriptionPackage, on_delete=models.CASCADE, db_constraint=False
    )
    subscriptionpackageservice = models.ForeignKey(
        SubscriptionPackageService, on_delete=models.CASCADE, db_constraint=False
    )
    keep_till = models.IntegerField(default=999)

    if settings.DB_ROUTING == 1:
        objects = SubscriptionPackageLinkServiceManager()

    class Meta:
        db_table = 'contracts_subscriptionpackage_services'

    def save(self, *args, **kwargs):
        if settings.DB_ROUTING == 1:
            if self.pk:
                # If args are provided, use the fields in args; otherwise, fallback to all model fields
                if args:
                    fields = args[0]  # Use the provided list in args
                else:
                    fields = [
                        field.name for field in self._meta.get_fields()
                    ]  # Fallback to all fields

                # Iterate through the fields
                fields_to_update = {}
                for field_name in fields:
                    # Check if the field exists on the model
                    if hasattr(self, field_name):
                        field_value = getattr(self, field_name)

                        # Get the actual field object by field name
                        field = self._meta.get_field(field_name)

                        # Skip ManyToManyFields
                        if isinstance(field, models.ManyToManyField) or isinstance(
                            field, models.ManyToOneRel
                        ):
                            continue  # Skip processing ManyToMany fields

                        # Check if the field is a ForeignKey
                        if isinstance(field, models.ForeignKey):
                            # Get the ForeignKey ID instead of the object
                            fk_value = getattr(self, field_name + "_id")
                            fields_to_update[field_name + "_id"] = fk_value
                        else:
                            # For non-ForeignKey fields, store the value
                            fields_to_update[field_name] = field_value

                # If there are fields to update, build and execute the SQL query
                if fields_to_update:
                    set_clause = ", ".join(
                        [f"{field} = %s" for field in fields_to_update.keys()]
                    )
                    values = list(fields_to_update.values())
                    values.append(
                        self.id
                    )  # Add the id to the values list for the WHERE clause

                    if self.keep_till < 1000:
                        old_instance = SubscriptionPackageLinkService.objects.filter(
                            id=self.id
                        ).first()
                        if old_instance:
                            sql_query = f"UPDATE contracts_subscriptionpackage_services SET {set_clause} WHERE id = %s"
                    else:
                        new_instance = SubscriptionPackageNew.objects.filter(
                            id=self.id
                        ).first()
                        if new_instance:
                            sql_query = f"UPDATE contracts_subscriptionpackage_services_new SET {set_clause} WHERE id = %s"
                    with connection.cursor() as cursor:
                        cursor.execute(sql_query, values)

            else:
                try:
                    self._meta.db_table = 'contracts_subscriptionpackage_services_new'
                    super().save(*args, **kwargs)

                except Exception as e:
                    logging.error(f"Error during saving the new instance: {str(e)}")
                    raise
        else:
            try:
                super().save(*args, **kwargs)

            except Exception as e:
                logging.error(f"Error during saving the old instance: {str(e)}")
                raise


class SubscriptionPackageServiceAttributeQuerySet(models.QuerySet):
    def _as_sql(self, *args, **kwargs):
        # Call the original method to get the SQL query
        sql, params = super()._as_sql(*args, **kwargs)
        # Replace the table name in the SQL query
        sql = sql.replace(
            'contracts_subscriptionpackage_services',
            'view_contracts_subscriptionpackage_services',
        )
        return sql, params

    def filter(self, *args, **kwargs):

        router.db_for_read(self.model)  # Force router evaluation
        # Replace `contracts_subscriptionpackage` with `combinedView` in the filter conditions
        filters = {}

        for key, value in kwargs.items():
            # Modify the FK filters to replace the incorrect table reference
            new_key = key.replace(
                'contracts_subscriptionpackage_services',
                'view_contracts_subscriptionpackage_services',
            )
            filters[new_key] = value
            # Modify select related parts if select is used
            if 'select' in kwargs:
                select_fields = kwargs.get('select', {})
                new_select_fields = {}
                for field, value in select_fields.items():
                    # Replace the table reference in the select fields
                    new_field = field.replace(
                        'contracts_subscriptionpackage_services',
                        'view_contracts_subscriptionpackage_services',
                    )
                    new_select_fields[new_field] = value
                kwargs['select'] = new_select_fields
            # Handle the case where related fields might still reference the old table
            if 'select_related' in kwargs:
                select_related_fields = kwargs['select_related']
                # Force router evaluation for all related models
                for related_field in select_related_fields:
                    related_model = self.model._meta.get_field(
                        related_field
                    ).related_model
                    router.db_for_read(related_model)

            if 'prefetch_related' in kwargs:
                prefetch_related_fields = kwargs['prefetch_related']
                # Force router evaluation for all related models
                for related_field in prefetch_related_fields:
                    related_model = self.model._meta.get_field(
                        related_field
                    ).related_model
                    router.db_for_read(related_model)

        return super().filter(*args, **filters)


class SubscriptionPackageServiceAttributeManager(models.Manager):
    def get_queryset(self):
        if settings.DB_ROUTING == 1:
            # Ensure that the router is invoked here for the base model
            router.db_for_read(self.model)
            # Return a custom queryset that operates on combinedView
            return SubscriptionPackageServiceAttributeQuerySet(
                self.model, using=self._db
            )
        return super().get_queryset()


class SubscriptionPackageServiceAttribute(DWHDumpable):
    id = models.BigAutoField(primary_key=True)
    value = models.CharField(max_length=1024, null=True)
    subscription_package_service = models.ForeignKey(
        'contracts.SubscriptionPackageService',
        related_name='attributes',
        on_delete=models.CASCADE,
    )
    attribute = models.ForeignKey('products.ServiceAttribute', on_delete=models.CASCADE)
    keep_till = models.IntegerField(default=999)

    def __str__(self):
        return '{}={}'.format(self.attribute.code, self.value)

    if settings.DB_ROUTING == 1:
        objects = SubscriptionPackageServiceAttributeManager()

    def save(self, *args, **kwargs):
        if settings.DB_ROUTING == 1:
            if self.pk:
                # If args are provided, use the fields in args; otherwise, fallback to all model fields
                if args:
                    fields = args[0]  # Use the provided list in args
                else:
                    fields = [
                        field.name for field in self._meta.get_fields()
                    ]  # Fallback to all fields

                # Iterate through the fields
                fields_to_update = {}
                for field_name in fields:
                    # Check if the field exists on the model
                    if hasattr(self, field_name):
                        field_value = getattr(self, field_name)

                        # Get the actual field object by field name
                        field = self._meta.get_field(field_name)

                        # Skip ManyToManyFields
                        if isinstance(field, models.ManyToManyField) or isinstance(
                            field, models.ManyToOneRel
                        ):
                            continue  # Skip processing ManyToMany fields

                        # Check if the field is a ForeignKey
                        if isinstance(field, models.ForeignKey):
                            # Get the ForeignKey ID instead of the object
                            fk_value = getattr(self, field_name + "_id")
                            fields_to_update[field_name + "_id"] = fk_value
                        else:
                            # For non-ForeignKey fields, store the value
                            fields_to_update[field_name] = field_value

                # If there are fields to update, build and execute the SQL query
                if fields_to_update:
                    set_clause = ", ".join(
                        [f"{field} = %s" for field in fields_to_update.keys()]
                    )
                    values = list(fields_to_update.values())
                    values.append(
                        self.id
                    )  # Add the id to the values list for the WHERE clause

                    if self.keep_till < 1000:
                        old_instance = SubscriptionPackageServiceAttribute.objects.filter(
                            id=self.id
                        ).first()
                        if old_instance:
                            sql_query = f"UPDATE contracts_subscriptionpackageserviceattribute SET {set_clause} WHERE id = %s"
                    else:
                        new_instance = SubscriptionPackageServiceAttributeNew.objects.filter(
                            id=self.id
                        ).first()
                        if new_instance:
                            sql_query = f"UPDATE contracts_subscriptionpackageserviceattribute_new SET {set_clause} WHERE id = %s"
                    with connection.cursor() as cursor:
                        cursor.execute(sql_query, values)

            else:
                try:
                    self._meta.db_table = (
                        'contracts_subscriptionpackageserviceattribute_new'
                    )
                    super().save(*args, **kwargs)

                except Exception as e:
                    logging.error(f"Error during saving the new instance: {str(e)}")
                    raise
        else:
            try:

                super().save(*args, **kwargs)

            except Exception as e:
                logging.error(f"Error during saving the old instance: {str(e)}")
                raise


class SubscriptionPackageNew(DWHDumpable):
    """
    Replaces Subscription.packages many to many relationship with
    join table which provides information about past packages,
    their activation and deactivation times.

    Services attributes are stored in a proper relational database
    form.

    Non-Recurrent packages are stored with exactly same activation
    and deactivation times.

    `activated_by`
        The memo id of which issued the change. This can
        be null if change was done via unknown methods.

    `deactivated_by`
        The memo id of which issued the change.
        Note that this can be NULL for deactivated services
        if the deactivation was done outside Memos.

    `grace_period`
        Number of days as grace period before the package gets
        deactivated.

    `grace_period_start_at`
        The start date of the grace period for the package.

    `fee`
        The fee for the package at the time of activation.

    `billing_account`
        Foreign Key to billing account of the subscription which is
        related to the package activation. Information is used by
        BSSAPI.

    `product_incentives_counter`
        Number of instance where package has been re-purchased using the
        accumulation volume mode.

    `suspended_at`
        Possible point of time the package was suspended.

    `keep_till`
        Data to be persisted till the value before removing/truncating/purging the partition.
    """

    objects: SubscriptionPackageManager

    if settings.DB_ROUTING == 1:
        objects = SubscriptionPackageManagerNew()
    else:
        objects = SubscriptionPackageManager()

    id = models.BigAutoField(primary_key=True)
    subscription = models.ForeignKey(
        'contracts.Subscription',
        db_index=True,
        related_name='xref_packages_new',
        on_delete=models.CASCADE,
    )
    package = models.ForeignKey(
        'products.Package', db_index=True, on_delete=models.CASCADE
    )
    activated_at = models.DateTimeField(default=datetime_now)
    deactivated_at = models.DateTimeField(default=None, null=True)
    services = models.ManyToManyField(
        'contracts.SubscriptionPackageServiceNew',
        through='contracts.SubscriptionPackageLinkServiceNew',
    )
    activated_by = models.BigIntegerField(null=True)
    deactivated_by = models.BigIntegerField(null=True)
    grace_period = models.IntegerField(default=None, null=True)
    grace_period_start_at = models.DateTimeField(default=None, null=True)
    fee = models.DecimalField(decimal_places=2, max_digits=32, default=0.0)
    billing_account = models.ForeignKey(
        'contracts.BillingAccount', null=True, on_delete=models.DO_NOTHING
    )
    package_offer = models.ForeignKey(
        'offers.PackageOffer', null=True, on_delete=models.DO_NOTHING
    )
    product_incentives_counter = models.IntegerField(default=0)
    suspended_at = models.DateTimeField(default=None, null=True, db_index=True)
    keep_till = models.IntegerField(default=999)

    def deactivate(
        self,
        deactivated_by: Optional[Memo] = None,
        deactivated_at: Optional[datetime] = None,
        package: Optional[Package] = None,
        keep_till: Optional[int] = None,
    ):
        """
        Set deactivation time and cascade down to related models.

        :param deactivated_by: The possible memo which triggered the
            change.
        :param deactivated_at: Set deactivation time explicitly.
            Otherwise use memos executed_at time or fall back to
            current time.
        :param package: Package to be deactivated, also used for calculation of keep_till partition value
        :param keep_till: The value for retention of partition
        """
        self.deactivated_at = _action_time(deactivated_by, deactivated_at)
        self.deactivated_by = deactivated_by.id if deactivated_by else None
        if not keep_till:
            self.keep_till = self.subscription.calculate_keep_till(
                package, self.deactivated_at
            )
        else:
            self.keep_till = keep_till
        self.suspended_at = None
        for service in self.services.all():
            service.deactivate(
                deactivated_by,
                deactivated_at=self.deactivated_at,
                package=package,
                keep_till=self.keep_till,
            )
        self.save()

    def suspend(self, deactivated_by: Optional[Memo] = None):
        """
        Set the package as suspended by setting the suspension time.

        :param deactivated_by: The possible memo which triggered the
            change.
        """
        self.suspended_at = _action_time(deactivated_by)
        self.save()

    def resume(self):
        """
        Remove the package suspension by setting suspension time as
        None.
        """
        self.suspended_at = None
        self.save()

    def __str__(self):
        return '{},activated_at={},deactivated_at={}'.format(
            self.package.code, self.activated_at, self.deactivated_at
        )

    class Meta:
        indexes = [
            models.Index(fields=['-activated_at']),
            models.Index(fields=['-deactivated_at']),
        ]
        db_table = "contracts_subscriptionpackage_new"


class SubscriptionPackageServiceNew(DWHDumpable):
    """
    `activated_by`
        The memo id which issued the change. This can
        be null if change was done via unknown methods OR without
        memo being present. One example of such use case is
        updates coming from CommonAPI (Zattoo).

        Note: Those should be adapted to use Memo as well.

    `deactivated_by`
        The memo id which issued the change.
        Note that this can be NULL for deactivated services
        if the deactivation was done outside Memos.

    `keep_till`
        Data to be kept alive till the value before removing/truncating/purging the partition.
    """

    objects: PackageServiceManager
    if settings.DB_ROUTING == 1:
        objects = PackageServiceManagerNew()
    else:
        objects = PackageServiceManager()

    id = models.BigAutoField(primary_key=True)
    subscription = models.ForeignKey(
        'contracts.Subscription', db_index=True, on_delete=models.CASCADE
    )
    service = models.ForeignKey(
        'products.Service', db_index=True, on_delete=models.CASCADE
    )
    activated_at = models.DateTimeField(default=datetime_now)
    deactivated_at = models.DateTimeField(default=None, null=True)
    activated_by = models.BigIntegerField(null=True)
    deactivated_by = models.BigIntegerField(null=True)
    suspended_at = models.DateTimeField(default=None, null=True)
    keep_till = models.IntegerField(default=999)

    class Meta:
        db_table = "contracts_subscriptionpackageservice_new"

    def deactivate(
        self,
        deactivated_by: Optional[Memo] = None,
        deactivated_at: Optional[datetime] = None,
        package: Optional[Package] = None,
        keep_till: Optional[int] = None,
    ):
        """
        Set deactivation time.

        :param deactivated_by: The possible memo which triggered the
            change.
        :param deactivated_at: Set deactivation time explicitly.
            Otherwise use memos executed_at time or fall back to
            current time.
        :param package: Package to be deactivated, also used for calculation of keep_till partition value
        :param keep_till: The value for retention of partition
        """
        self.deactivated_at = _action_time(deactivated_by, deactivated_at)
        self.deactivated_by = deactivated_by.id if deactivated_by else None
        if not keep_till:
            self.keep_till = self.subscription.calculate_keep_till(
                package, self.deactivated_at
            )
        else:
            self.keep_till = keep_till
        self.save()

    def save_new_attributes(self, attrs):
        """
        .. NOTE:: Quick solution, do not use if possible!

        :param dict attrs:
            key must be a attribute code
            value must be attribute value
        """
        switched_at = datetime_now()
        self.deactivate(deactivated_at=switched_at)

        sps = SubscriptionPackageService.objects.create(
            subscription=self.subscription,
            service=self.service,
            activated_at=switched_at,
        )
        # add new subscription package service to all subscription packages
        for spkg in self.subscriptionpackage_set.all():
            spkg.services.add(sps)
        # create new attributes
        for attr_code, value in attrs.items():
            if not value:
                continue
            attr = pc.ServiceAttribute.objects.get(code=attr_code)
            SubscriptionPackageServiceAttribute.objects.create(
                subscription_package_service=sps, attribute=attr, value=value
            )

    def suspend(self, deactivated_by: Optional[Memo] = None):
        """
        Set the package as suspended by setting the suspension time.

        :param deactivated_by: The possible memo which triggered the
            change.
        """
        self.suspended_at = _action_time(deactivated_by)
        self.save()

    def resume(self):
        """
        Remove the package suspension by setting suspension time as
        None.
        """
        self.suspended_at = None
        self.save()

    def __str__(self):
        return 'service={},activated_at={},deactivated_at={}'.format(
            self.service.code, self.activated_at, self.deactivated_at
        )


class SubscriptionPackageLinkServiceNew(models.Model):
    subscriptionpackage = models.ForeignKey(
        SubscriptionPackageNew, on_delete=models.CASCADE, db_constraint=False
    )
    subscriptionpackageservice = models.ForeignKey(
        SubscriptionPackageServiceNew, on_delete=models.CASCADE, db_constraint=False
    )
    keep_till = models.IntegerField(default=999)

    if settings.DB_ROUTING == 1:
        objects = SubscriptionPackageLinkServiceManager()

    class Meta:
        db_table = 'contracts_subscriptionpackage_services_new'


class SubscriptionPackageServiceAttributeNew(DWHDumpable):
    id = models.BigAutoField(primary_key=True)
    value = models.CharField(max_length=1024, null=True)
    subscription_package_service = models.ForeignKey(
        'contracts.SubscriptionPackageServiceNew',
        related_name='attributes',
        on_delete=models.CASCADE,
    )
    attribute = models.ForeignKey('products.ServiceAttribute', on_delete=models.CASCADE)
    keep_till = models.IntegerField(default=999)
    if settings.DB_ROUTING == 1:
        objects = SubscriptionPackageServiceAttributeManager()

    class Meta:
        db_table = "contracts_subscriptionpackageserviceattribute_new"
