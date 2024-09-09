# Defaults
#   -> Default definitions for values across the package
#
SAILDB.DEF = list(
  ## Default keychain name
  KEYCHAIN          = 'SAILDB',
  ## Default database to connect with
  DATABASE          = 'PR_SAIL',
  ## Default DB2 code page value; defaults to UTF-8 encoding
  CODEPAGE.VAL      = 1208L,
  ## Default DB2 code page system env variable name
  CODEPAGE.VAR      = 'DB2CODEPAGE',
  ## Maximum table name length
  MAX.TBL.NAME.LEN  = 127L,
  ## Maximum varchar character length
  MAX.CHAR.LEN      = 32700L,
  ## Regex to match & validate columns names
  COLUMN.NAME.REGEX = '^([a-zA-Z][a-zA-Z0-9_\\-]*)$',
  ## Regex to match & validate `[schema_name].[table_name]`
  TABLE.NAME.REGEX  = '^([a-zA-Z][a-zA-Z0-9_\\-]*)\\.([a-zA-Z][a-zA-Z0-9_\\-]*)$',
  ## Minimum chunk size to use when planning inserts and/or creation statements
  MIN.CHUNK.SIZE    = 1000L,
  ## Default timezone name
  TIMEZONE          = 'UTC',
  ## Coercible datetime information
  DATETIME.PROFILE  = list(
    POSIXct  = list(datatype='TIMESTAMP', format='%Y-%m-%d-%H.%M.%OS6'),
    Date     = list(datatype='DATE',      format='%Y-%m-%d'),
    ITime    = list(datatype='TIME',      format='%H:%M:%OS')
  ),
  ## Coercible datetimes objects and their expected formats
  DATETIME.FORMATS  = list(
    POSIXct  = c('%Y-%m-%d %H:%M:%OS6', '%Y-%m-%d %H:%M:%OS', '%Y-%m-%d %H:%M'),
    Date     = c('%Y-%m-%d', '%Y%m%d'),
    ITime    = c('%H:%M', '%H:%M:%OS')
  ),
  ## Name of the user-defined dataset reference class used to construct a `DatasetContainer` instance
  DUDF.CLASS        = 'DatasetItem',
  ## Name of the metadata reference class used to construct a `DatasetContainer` instance
  DREF.CLASS        = 'DatasetReference',
  ## Date format used for refresh date interpolation
  DREF.FMT          = '%Y%m%d',
  ## Regex to extract the date suffix appended to refresh tables
  DREF.RE.REFRESH   = '_([\\d]{4})([\\d]{2})([\\d]{2})$',
  ## Maps the dataset and/or table relation type to a regex validation pattern
  DREF.RELATION     = list(
    BASE       = '^BASE',
    REFERENCE  = '^SAIL[A-Z][\\w]+$',
    SESSION    = '^SESSION$',
    PROJECT    = '^SAIL\\d+V$',
    WORKSPACE  = '^SAILW\\d+V$',
    ENCRYPTION = '^SAILX\\d+V$'
  ),
  ## Table privileges, used for validation by `DatasetContainer`
  PRIVILEGES        = list(
    'SELECT',
    'UPDATE',
    'INSERT',
    'DELETE',
    'ALTER'
  )
)


# Messages
#   -> Client-related communication
#
SAILDB.MSGS = list(
  ## Various messages and/or names used as either (a) defaults; or (b) communication with the client
  PKG     = 'SAILDB',
  EMPTY   = '[NULL]',
  UNKNOWN = 'Unknown error occurred',
  NAMEERR = 'Unknown validation error with given name',
  CONFIRM = 'OK',
  REJECT  = 'Cancel',
  ## Messages relating to `rstudioapi::showDialogue` calls via the `confirm.client` utility method
  AUTH    = list(
    RM.KEYS = list(
      title   = 'Authentication failure',
      message = 'Your connection was refused. This might be due to an incorrect password, would you like to clear your current profile\'s secrets?',
      confirm = 'Remove secrets',
      cancel  = 'Cancel'
    ),
    RETRY = list(
      title   = 'Authentication failure',
      message = 'Your connection was refused, would you like to try again?',
      confirm = 'Retry',
      cancel  = 'Cancel'
    ),
    SAVE = list(
      title   = 'Authenticated',
      message = 'Would you like to save your current connection to your device?\n\nThis will allow you open future connections without having to supply your username and password',
      confirm = 'Yes',
      cancel  = 'No'
    )
  )
)


# Prompts
#   -> Prompt definitions for the `prompt.client` utility method
#
SAILDB.PROMPTS = list(
  PROMPT   = list(
    title   = SAILDB.MSGS$PKG,
    message = 'Enter value:',
    default = ''
  ),
  SECRET   = list(
    name    = 'Secret',
    title   = SAILDB.MSGS$PKG,
    message = 'Enter secret:'
  ),
  USERNAME = list(
    title   = SAILDB.MSGS$PKG,
    message = 'Enter username:',
    default = ''
  ),
  PASSWORD = list(
    message = 'Enter password:'
  )
)
