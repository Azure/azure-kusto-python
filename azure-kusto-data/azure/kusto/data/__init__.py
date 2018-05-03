# review: why the exposure here? Why not just expose them from their modules
# directly? If you don't want any other part of the submodules of this class
# to not be public then you need to add a '_' to the front of the modules.
# Otherwise consider brings these classes into this module if you feel the need
# keep it all separate. Otherwise the other modules are small enough to
# drop the data package and just have a data.py submodule.
from .kusto_client import KustoClient, KustoResultIter, KustoResponse
from .kusto_exceptions import KustoError, KustoClientError, KustoServiceError
from .version import VERSION as __version__
