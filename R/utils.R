#' @description
#' Compares the OS platform of the R environment to the desired platform
#'
#' @param desired.platform (\code{character})\cr
#'   A scalar character string specifying the desired platform name
#'
#' @return A logical describing whether the current platform is the desired platform
#'
is.platform <- function (desired.platform) {
  res = FALSE
  ros = .Platform$OS.type
  attr(res, 'platform') = ros

  if (!rlang::is_scalar_character(desired.platform)) {
    return (res)
  }

  return (ifelse(.Platform$OS.type == desired.platform, !res, res))
}


#' @description
#' Determines a value is 'empty', \emph{i.e.} is one of: \code{NULL | NA}
#'
#' @param x (\code{any})\cr
#'   Some value to test
#'
#' @return A logical describing whether the value is considered to be 'empty'
#'
is.empty <- function (x) {
  return (all(is.null(x) | is.na(x)))
}


#' @description
#' Determines whether a list - or vector, if specified - is a named list as specified by the \code{condition} argument
#'
#' @param x (\code{any})\cr
#'   Some value to test
#' @param condition (\code{character|NA})\cr
#'   An optional condition describing whether all or any of the values of the list must be named
#' @param accept.named.vecs (\code{logical})\cr
#'   An optional flag to specify whether named vectors are acceptable; defaults to \code{FALSE}
#'
#' @return A logical reflecting whether the value is a named list; will also return \code{FALSE} for non-list values
#'
is.named.list <- function (x, condition = 'ALL', accept.named.vecs = FALSE) {
  condition = ifelse(rlang::is_scalar_character(condition), toupper(condition), NA)
  if (is.na(condition) || !(condition %in% c('ALL', 'ANY'))) {
    return (FALSE)
  }

  if (!rlang::is_atomic(accept.named.vecs) || !rlang::is_logical(accept.named.vecs)) {
    accept.named.vecs = FALSE
  }

  if (!rlang::is_list(x) && !(accept.named.vecs && rlang::is_vector(x))) {
    return (FALSE)
  }

  if (condition == 'ALL') {
    return (length(x) == sum(names(x) != '', na.rm=TRUE))
  } else if (condition == 'ANY') {
    return (sum(names(x) != '', na.rm=TRUE) > 0)
  }

  return (FALSE)
}


#' @description
#' Determines whether a scalar character value is empty or contains only spaces
#'
#' @param x (\code{character|any})\cr
#'   Some value to consider
#' @param trim.spaces (\code{logical})\cr
#'   Whether to check if the string contains only spaces; optional, defaults to \code{TRUE}
#'
#' @return A logical reflecting whether the value is an empty string; will also return \code{TRUE} for non-string values
#'
is.string.empty <- function (x, trim.spaces = TRUE) {
  if (is.empty(x) || !rlang::is_scalar_character(x)) {
    return (TRUE)
  }

  if (trim.spaces) {
    x = trimws(x)
  }

  return (nchar(x) == 0L)
}


#' @description
#' Sanitise the given string so that it meets DB2 SQL name requirements for both columns & tables
#'
#' @param x (\code{character})\cr
#'   The name to sanitise
#'
#' @return A sanitised string
#'
sanitise.string.name <- function (x) {
  return (base::gsub('[^[:alnum:]_]+', '', x))
}


#' @description
#' Attempts to derive both a schema name and a table name as delimited by the dot punctuation. These are extracted
#' and validated through the \code{TABLE.NAME.REGEX} as defined within \code{SAILDB.DEF}; and the character length of
#' the table name is validated if an appropriate \code{max.table.length} is defined
#'
#' @param x (\code{character})\cr
#'   The prospective table reference
#' @param transform.upper (\code{logical})\cr
#'   Whether to return the components in an upper case format
#' @param max.table.length (\code{integer|any})\cr
#'   An optional integer defining the maximum character length of a table name; defaults to \code{SAILDB.DEF$MAX.TBL.NAME.LEN}
#'
#' @return Either:
#'   \enumerate{
#'     \item If valid: a list describing the table reference components in the shape of \code{{ schema: string, table: string }}
#'     \item If invalid: a \code{FALSE} logical value with a \code{reason} attribute describing the validation issue(s)
#'   }
#'
get.table.reference <- function (x, transform.upper = TRUE, max.table.length = SAILDB.DEF$MAX.TBL.NAME.LEN) {
  res = FALSE
  if (!rlang::is_scalar_character(x)) {
    attr(res, 'reason') = stringr::str_interp('Expected string but got ${typeof(x)} with Repr<${toString(x)}>')
    return (res)
  }

  validator = stringr::regex(
    SAILDB.DEF$TABLE.NAME.REGEX,
    ignore_case=TRUE,
    multiline=FALSE
  )

  results = stringr::str_match(x, validator)
  results.invalid = (
    (!is.matrix(results) || nrow(results) != 1L || ncol(results) != 3L)
    || any(sapply(as.list(results[1, 2:3]), function (x) (is.na(x) || is.string.empty(x))) == TRUE)
  )

  if (results.invalid) {
    attr(res, 'reason') = stringr::str_interp('Failed to match `[SCHEMA].[TABLE]` from \'${x}\' - must validate against Regex<${SAILDB.DEF$NAME.VALIDATOR}>')
    return (res)
  }

  schema = results[1, 2]
  table = results[1, 3]
  if (is.numeric(max.table.length) && nchar(x) > max.table.length) {
    attr(res, 'reason') = stringr::str_interp('Name<value: \'${table}\', length: ${nchar(x)}> exceeds maximum char limit of ${max.table.length}')
    return (res)
  }

  if (transform.upper) {
    schema = toupper(schema)
    table = toupper(table)
  }

  return (list(schema=schema, table=table))
}


#' @description
#' Determines whether a table reference such as \code{[SCHEMA_NAME].[TABLE_NAME]} is valid by attempting to extract a valid schema name and table name on both the left-hand and
#' right-hand side of the punctuation delimiter
#'
#' @param x (\code{character})\cr
#'   The table reference to validate
#' @param max.table.length (\code{integer|any})\cr
#'   An optional integer defining the maximum character length of a table name; defaults to \code{SAILDB.DEF$MAX.TBL.NAME.LEN}
#'
#' @return A logical reflecting whether the given table reference is valid
#'
is.valid.table.reference <- function (x, max.table.length = SAILDB.DEF$MAX.TBL.NAME.LEN) {
  result = get.table.reference(x, max.table.length=max.table.length)
  if (is.list(result)) {
    return (TRUE)
  }

  return (result)
}


#' @description
#' Attempts to derive the dataset relation from a schema name
#'
#' @param schema (\code{character})\cr
#'   The schema from which to derive the relation tpe
#'
#' @return Either:
#'   \enumerate{
#'     \item If valid: a scalar character describing the relation type
#'     \item If invalid: a \code{FALSE} logical value with a \code{reason} attribute describing the validation issue(s)
#'   }
#'
get.dataset.relation <- function (schema) {
  res = FALSE
  if (!rlang::is_scalar_character(schema) || is.string.empty(schema)) {
    attr(res, 'reason') = 'Failed to derive dataset relation, the schema specified is malformed; expected non-empty, scalar character string'
    return (res)
  }

  relation.types = SAILDB.DEF$DREF.RELATION
  relation.names = names(relation.types)
  for (i in 1:length(relation.types)) {
    name = relation.names[i]
    pattern = stringr::regex(unlist(relation.types[i]), ignore_case=TRUE, multiline=FALSE)
    results = stringr::str_match(schema, pattern)

    results.invalid = (
      (!is.matrix(results) || nrow(results) != 1L || ncol(results) < 1L)
      || any(sapply(as.list(results[1, 1:length(ncol(results))]), function (x) (is.na(x) || is.string.empty(x))) == TRUE)
    )

    if (results.invalid) {
      next
    }

    return (name)
  }

  return ('UNKNOWN')
}


#' @description
#' Attempts to parse the components associated with a user-defined dataset reference
#'
#' @param x (\code{character})\cr
#'   The prospective dataset reference
#' @param transform.upper (\code{logical})\cr
#'   Whether to return the table reference components in an upper case format
#' @param max.table.length (\code{integer|any})\cr
#'   An optional integer defining the maximum character length of a table name; defaults to \code{SAILDB.DEF$MAX.TBL.NAME.LEN}
#'
#' @return Either:
#'   \enumerate{
#'     \item If valid: a list describing the dataset reference components
#'     \item If invalid: a \code{FALSE} logical value with a \code{reason} attribute describing the validation issue(s)
#'   }
#'
parse.dataset.components <- function (x, transform.upper = TRUE, max.table.length = SAILDB.DEF$MAX.TBL.NAME.LEN) {
  # Attempt to parse table reference; return reason attr obj on failure
  table.ref = get.table.reference(x, transform.upper=transform.upper, max.table.length=max.table.length)
  if (is.empty(table.ref)) {
    return (table.ref)
  }

  # Attempt to derive dataset relation; return reason attr obj on failure
  relation = get.dataset.relation(table.ref$schema)
  if (is.empty(table.ref)) {
    return (relation)
  }

  # Parse component(s)
  date.regex = stringr::regex(SAILDB.DEF$DREF.RE.REFRESH, ignore_case=TRUE, multiline=FALSE)
  date.suffix = stringr::str_match(table.ref$table, date.regex)
  date.invalid = (
    (!is.matrix(date.suffix) || nrow(date.suffix) != 1L || ncol(date.suffix) != 4L)
    || any(sapply(as.list(date.suffix[1, 2:4]), function (x) (is.na(x) || is.string.empty(x))) == TRUE)
  )

  result.tag = FALSE
  result.date = NA
  result.name = table.ref$table
  if (!date.invalid) {
    date.string = do.call(stringr::str_c, as.list(unlist(date.suffix[1, 2:4], use.names=FALSE)))
    result.date = get.datetime.from.string(date.string, fmt=SAILDB.DEF$DREF.FMT)

    if (inherits(result.date, 'Date')) {
      result.tag = '_${date}'
      result.name = base::gsub(stringr::str_c('_', date.string, '$'), '', result.name)
    } else {
      result.date = NA
    }
  }

  return (list(
    tag      = result.tag,
    date     = result.date,
    name     = result.name,
    table    = table.ref$table,
    schema   = table.ref$schema,
    static   = !is.na(result.date),
    relation = relation
  ))
}


#' @description
#' Interpolate metadata into a \code{[SCHEMA].[TABLE]} reference
#'
#' @param reference (\code{list|DatasetReference|DatasetItem})\cr
#'   Some \code{list | DatasetReference | DatasetItem} to interpolate into a \code{[SCHEMA].[TABLE]} reference
#' @param as.table.name (\code{logical|NA})\cr
#'   Determines whether both the schema & table are interpolated or if just the table name is interpolated
#'
#' @return A character string value
#'
interpolate.reference <- function (reference, as.table.name = FALSE) {
  as.table.name = coerce.boolean(as.table.name)
  if (!rlang::is_list(reference) && !inherits(reference, SAILDB.DEF$DREF.CLASS) && !inherits(reference, SAILDB.DEF$DUDF.CLASS)) {
    return ('')
  }

  data = reference$dataset
  date = reference$date
  table = data$name
  schema = reference$schema

  if (rlang::is_scalar_character(data$tag) && (inherits(date, 'Date') || inherits(date, 'POSIXct'))) {
    date = format(date, SAILDB.DEF$DREF.FMT)
    table = stringr::str_interp(stringr::str_c('${table}', data$tag))
  }

  if (rlang::is_scalar_character(data$alt)) {
    table = stringr::str_interp('${data$alt}_${table}')
  }

  if (as.table.name) {
    return (table)
  }

  return (stringr::str_c(schema, '.', table))
}


#' @description
#' Check if the input is a string that can be converted to a date, time or datetime
#'
#' @param x (\code{character})\cr
#'   Some value to consider
#' @param fmt (\code{character|list|NA})\cr
#'   Either (a) a datetime format string, \emph{e.g.} \code{%Y-%m-%d}; or (b) a named list of vectors describing the date object type and the associated formats - see \code{SAILDB.DEF$DATETIME.FORMATS}
#' @param tz (\code{character|NA})\cr
#'   Optional timezone; defaults to \code{option(SAILDB.TIMEZONE=SAILDB.DEF$TIMEZONE)}
#'
#' @return A logical reflecting whether the value can be coerced into a datetime-like object
#'
is.string.datetime <- function (x, fmt = NA, tz = getOption('SAILDB.TIMEZONE', SAILDB.DEF$TIMEZONE)) {
  res = try(suppressWarnings(get.datetime.from.string(x, fmt, tz)), silent=TRUE)
  return (!inherits(res, 'try-error'))
}


#' @description
#' Attempts to coerce the input string into a date, time or datetime object
#'
#' @param x (\code{character})\cr
#'   Some value to consider
#' @param fmt (\code{character|list})\cr
#'   Either (a) a datetime format string, \emph{e.g.} \code{%Y-%m-%d}, which defaults to the \code{Date} type; or (b) a named list of vectors describing the date object type and the associated formats - see \code{SAILDB.DEF$DATETIME.FORMATS}
#' @param tz (\code{character})\cr
#'   Optional scalar character specifying a timezone; defaults to \code{option(SAILDB.TIMEZONE=SAILDB.DEF$TIMEZONE)}
#'
#' @return Either (a) the matched object type; or (b) \code{NA} for values that cannot be coerced
#'
get.datetime.from.string <- function (x, fmt = NA, tz = getOption('SAILDB.TIMEZONE', SAILDB.DEF$TIMEZONE)) {
  if (is.string.empty(x)) {
    return (NA)
  }

  invalid.type = !is.empty(fmt) && !is.named.list(fmt) && !rlang::is_scalar_character(fmt)
  if (invalid.type) {
    type = class(fmt)[1]
    message = ifelse(rlang::is_list(fmt), 'Unnamed elements of list', stringr::str_interp('Malformed type, got ${type}'))
    rlang::abort(stringr::str_interp('Expected `fmt` parameter as one of character|list[named] but failed with error: ${message}'))
  }

  if (!is.empty(tz) && !(rlang::is_scalar_character(tz) || is.string.empty(tz))) {
    tz = class(tz)[1]
    rlang::abort(stringr::str_interp('Expected `tz` parameter as a non-empty, scalar character but got ${tz}'))
  }

  if (is.empty(tz)) {
    tz = getOption('SAILDB.TIMEZONE', SAILDB.DEF$TIMEZONE)
  }

  if (!is.empty(fmt) && rlang::is_scalar_character(fmt)) {
    datetime.object = as.Date(x, tz=tz, format=fmt)
    if (!is.na(datetime.object)) {
      attr(datetime.object, 'date.tz') = tz
      attr(datetime.object, 'date.fmt') = fmt
    }

    return (datetime.object)
  }

  if (is.empty(fmt)) {
    fmt = SAILDB.DEF$DATETIME.FORMATS
  }

  for (datetime.type in names(fmt)) {
    datetime.fmt = fmt[[datetime.type]]
    if (!is.vector(datetime.fmt)) {
      next
    }

    datetime.object = NA
    if (datetime.type == 'POSIXct') {
      datetime.object = as.POSIXct(x, tz=tz, tryFormats=datetime.fmt, optional=TRUE)
    } else if (datetime.type == 'Date') {
      datetime.object = as.Date(x, tz=tz, tryFormats=datetime.fmt, optional=TRUE)
    } else if (datetime.type == 'ITime') {
      for (elem in datetime.fmt) {
        value = try(as.ITime(c(x), tz=tz, format=elem), silent=TRUE)
        if (class(value)[1] == 'ITime') {
          datetime.fmt = elem
          datetime.object = value
          break
        }
      }
    }

    if (!is.na(datetime.object)) {
      attr(datetime.object, 'date.tz') = tz
      attr(datetime.object, 'date.fmt') = datetime.fmt

      return (datetime.object)
    }
  }

  return (NA)
}


#' @description
#' Wraps a value in a pair of single quotes to delimit characters, datetimes and logical values; and escapes any single quotations already present
#'
#' @param x (\code{character})\cr
#'   The value to process
#'
#' @return An escaped string representing the value, delimited by a pair of single quotation marks
#'
mark.safe.value <- function (x) {
  return (base::sQuote(base::gsub("'", "''", x), q=FALSE))
}


#' @description
#' Attempts to get derive both the \code{*args} and \code{**kwargs} from the given variadic arguments
#'
#' @param ... (\code{variadic arguments})\cr
#'   The given arguments
#'
#' @return A list of shape \code{{ args: list, kwargs: list, varargs: list }} as derived by the given arguments
#'
get.args <- function (...) {
  varargs = list(...)
  vanames = names(varargs)
  if (!is.null(vanames)) {
    vanames = lapply(vanames, function (x) (!rlang::is_scalar_character(x) || is.string.empty(x)))
    args <- unlist(varargs[vanames == TRUE], use.names=FALSE)
    kwargs <- varargs[vanames == FALSE]
  } else {
    args <- varargs
    kwargs <- list()
  }

  return (list(args=args, kwargs=kwargs, varargs=varargs))
}


#' @description
#' Attempts to parse the specified parameters from the given var args
#'
#' @param params (\code{vector})\cr
#'   A vector of character strings describing the argument names
#' @param ... (\code{variadic arguments})\cr
#'   The given arguments
#'
#' @return A key-value pair list describing the parameters
#'
parse.params <- function (params, ...) {
  varargs = get.args(...)

  args = varargs$args
  kwargs = varargs$kwargs

  key.diff = setdiff(params, names(kwargs))
  if (length(key.diff) < 1) {
    result <- kwargs
  } else if (length(kwargs) > 0) {
    result <- kwargs

    for (i in 1:length(args)) {
      len = length(key.diff)
      if (len < 1) {
        break
      }

      result[[key.diff[1]]] <- args[i]
      key.diff = tail(key.diff, len - 1)
    }
  } else {
    result <- head(args, pmin(length(args), length(params)))
    names(result) <- head(params, pmin(length(args), length(params)))
  }

  return (result)
}


#' @description
#' Coerces a single logical type from the given value
#'
#' @param x (\code{logical|any})\cr
#'   Some value to consider
#' @param allow.nanull (\code{logical})\cr
#'   Whether to consider values with a type of \code{[ NA | NULL ]} as a logical type; defaults to \code{FALSE}
#' @param nanull.is.falsy (\code{logical})\cr
#'   Determines whether a \code{NA | NULL} value is considered as a truthy or falsy value; only applicable if \code{allow.nanull} is flagged
#' @param default (\code{logical})\cr
#'   The default value; will resolve to \code{FALSE} if a non-logical type is provided
#'
#' @return A logical value derived from the given value
#'
coerce.boolean <- function (x, allow.nanull = FALSE, nanull.is.falsy = TRUE, default = FALSE) {
  if (rlang::is_logical(x)) {
    return (x)
  }

  default = ifelse(rlang::is_logical(default), default, FALSE)
  allow.nanull = ifelse(rlang::is_logical(allow.nanull), allow.nanull, FALSE)
  nanull.is.falsy = ifelse(rlang::is_logical(nanull.is.falsy), nanull.is.falsy, TRUE)
  if (allow.nanull && is.empty(x)) {
    return (!nanull.is.falsy)
  }

  return (default)
}


#' @description
#' Prompts the client for information
#'
#' @param type (\code{character})\cr
#'   The prompt type; must be one of the names defined by \code{SAILDB.PROMPTS}
#' @param message (\code{character|NA})\cr
#'   The prompt message i.e. a scalar character describing the content of the dialogue body
#' @param title (\code{character|NA})\cr
#'   The prompt title if applicable; see \code{SAILDB.PROMPTS} for available params
#' @param name (\code{character|NA})\cr
#'   The name of the secret if applicable; see \code{SAILDB.PROMPTS} for available params
#' @param default (\code{character|NA})\cr
#'   The placeholder text if applicable; see \code{SAILDB.PROMPTS} for available params
#' @param allow.empty (\code{logical|NA})\cr
#'   Whether to accept empty string values; if not, will return the \code{default.result} value
#' @param default.result (\code{any})\cr
#'   The default return value if the prompt fails, or if the user doesn't enter any information
#'
#' @return The user's input value if the type is validated & empty values are allowed; otherwise returns the \code{default.result} value (defaults to \code{NA})
#'
prompt.client = function (type, message = NA, title = NA, name = NA, default = NA, allow.empty = TRUE, default.result = NA) {
  if (!rlang::is_scalar_character(type)) {
    rlang::warn(paste0('Expected Parameter<type> as string but got ', class(type)))
    return (default.result)
  }

  ptype = toupper(type)
  types = names(SAILDB.PROMPTS)
  if (!(ptype %in% types)) {
    types = append(unlist(types, use.names=FALSE), list(sep=', '))
    rlang::warn(paste('Invalid prompt type, expected one of {', do.call(paste, types), '} but got', type, sep=' '))
    return (default.result)
  }

  method = switch(ptype,
    'PROMPT'   = rstudioapi::showPrompt,
    'SECRET'   = rstudioapi::askForSecret,
    'PASSWORD' = rstudioapi::askForPassword,
    'USERNAME' = rstudioapi::showPrompt
  )

  kwargs = list(message=message, title=title, name=name, default=default)
  params = SAILDB.PROMPTS[[ptype]]
  for (key in names(params)) {
    value = kwargs[[key]]
    if (!rlang::is_scalar_character(value)) {
      next
    }

    params[[key]] = value
  }

  if (ptype == 'PASSWORD') {
    names(params)[1] = 'prompt'
  }

  result = do.call(method, params)
  allow.empty = coerce.boolean(allow.empty, default=TRUE)
  if (allow.empty | !is.string.empty(result)) {
    return (result)
  }

  return (default.result)
}


#' @description
#' Prompts the user to confirm an action
#'
#' @param title (\code{character})\cr
#'   The prompt title
#' @param message (\code{character})\cr
#'   The prompt message i.e. a scalar character describing the content of the dialogue body
#' @param confirm (\code{character|NA})\cr
#'   Optional scalar character to override the confirmation button; defaults to \code{SAILDB.MSGS$CONFIRM}
#' @param cancel (\code{logical|NA})\cr
#'   Optional scalar character to override the cancel button button; defaults to \code{SAILDB.MSGS$REJECT}
#' @param default.result (\code{any})\cr
#'   The default return value if the prompt fails, or if the user doesn't enter any information
#'
#' @return A logical specifiying whether the user selected to confirm or cancel the confirmation prompt
#'
confirm.client = function (title, message, confirm = SAILDB.MSGS$CONFIRM, cancel = SAILDB.MSGS$REJECT, default.result = FALSE) {
  valid.params = all(lapply(list(title, message), rlang::is_scalar_character) == TRUE)
  if (!valid.params) {
    rlang::warn(paste0('Expected Parameters<title, message> as scalar characters but got [', do.call(paste, append(lapply(list(title, message), class), list(sep=', '))), ']'))
    return (default.result)
  }

  msg = try(
    suppressWarnings(rstudioapi::showQuestion(title, message, confirm, cancel)),
    silent=TRUE
  )

  return (coerce.boolean(msg, default=default.result))
}


#' @description
#' Attempts to signal a condition (warn, info)
#'
#' @param message (\code{character})\cr
#'   Some condition type, i.e. one of \code{[ info | warn ]}
#' @param message (\code{character})\cr
#'   Some condition message
#' @param call (\code{environment})\cr
#'   The execution environment of the caller
#' @param suppress.logs (\code{logical})\cr
#'   Whether to suppress message logs; defaults to \code{option(SAILDB.QUIET=FALSE)}
#' @param suppress.warnings (\code{logical})\cr
#'   Whether to suppress warnings; defaults to \code{option(SAILDB.NO.WARN=FALSE)}
#'
#' @return An invisible logical describing whether the condition was raised
#'
try.log <- function (type, message, call = NULL, suppress.logs = getOption('SAILDB.QUIET', FALSE), suppress.warnings = getOption('SAILDB.NO.WARN', FALSE)) {
  if (!rlang::is_scalar_character(type) || all(is.na(message) == TRUE)) {
    return (invisible(FALSE))
  }

  type = tolower(type)
  message = as.character(message)

  suppress.logs = coerce.boolean(suppress.logs, FALSE)
  suppress.warnings = coerce.boolean(suppress.warnings, FALSE)
  if ((type == 'info') & !suppress.logs) {
    return (rlang::inform(message, call=call))
  } else if ((type == 'warn') & !suppress.warnings) {
    return (rlang::warn(message, call=call))
  }

  return (invisible(FALSE))
}


#' @description
#' Attempts to throw an error, otherwise returns a reference value with a message attribute
#'
#' @param message (\code{character})\cr
#'   Some error message
#' @param call (\code{environment})\cr
#'   The execution environment of the caller
#' @param reference.value (\code{any})\cr
#'   Some return value with the message appended to it as an attribute
#' @param stop.on.error (\code{logical})\cr
#'   Whether to return a \code{FALSE} logical when an error is encountered instead of stopping the execution of the parent thread; defaults to \code{option(SAILDB.THROW.ERRORS=TRUE)}
#'
#' @return If not aborted: the given reference value +/- the reason attribute
#'
try.abort <- function (message, call = NULL, reference.value = FALSE, stop.on.error = getOption('SAILDB.THROW.ERRORS', TRUE)) {
  stop.on.error = coerce.boolean(stop.on.error, default=TRUE)
  if (!stop.on.error) {
    if (!is.null(reference.value)) {
      attr(reference.value, 'reason') = message
    }

    return (reference.value)
  }

  rlang::abort(message, call=call)
}
