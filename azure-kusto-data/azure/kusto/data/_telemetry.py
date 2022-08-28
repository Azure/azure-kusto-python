from azure.core.settings import settings


def add_adx_attributes(**kwargs):
    tracing_attributes = kwargs.pop("tracing_attributes", {})
    span_impl_type = settings.tracing_implementation
    if span_impl_type is not None:
        for key, val in tracing_attributes:
            span_impl_type.add_attribute(key, val)
