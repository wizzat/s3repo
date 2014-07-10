class RepoError(Exception): pass

class RepoInternalError(RepoError): pass

class RepoExternalError(RepoError): pass
class RepoAPIError(RepoError): pass

class RepoNoBackupsError(RepoExternalError): pass
class RepoFileNotUploadedError(RepoExternalError): pass

class RepoAlreadyExistsError(RepoError): pass
class RepoNoBackupsError(RepoError): pass
class RepoFileAlreadyExistsError(RepoError): pass
class RepoFileDoesNotExistLocallyError(RepoError): pass
class RepoUploadError(RepoError): pass
class RepoDownloadError(RepoError): pass
class PurgingPublishedRecordError(RepoError): pass
class NoConfigurationError(RepoError): pass
class RepoConcurrentInsertionError(RepoError): pass
