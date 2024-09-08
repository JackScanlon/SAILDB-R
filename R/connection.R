#' @title Connection
#'
#' @rdname SAIL-Connection
#'
#' @description
#' SAILR ODBC DBI connection
#'
#' @details
#' Available options:
#' \enumerate{
#'   \item \code{SAILR.USE.SECRETS}: Whether to use the SAILR::Profile manager to store & load secrets, i.e. a password storage manager for each username used to authenticate database connections
#'   \item \code{SAILR.DROP.UDF}: Specifies whether the \code{SAILR::Connection$drop()} method should use the SAIL user-defined function instead of using an anonymous procedure
#'   \item \code{SAILR.CHUNK.SIZE}: Describes the number of rows to be sent in each statement when saving a \code{data.frame} to the database; this can be reduced or increased depending on how wide your table structure is
#'   \item \code{SAILR.TIMEZONE}: Specifies the timezone used for \code{DATE}, \code{TIMESTAMP} and \code{DATE} columns
#'   \item \code{SAILR.QUIET}: Determines whether the \code{SAILR::Connection} methods will send condition messages when operations are started / finished; can be used to measure performance and/or debug statement(s)
#'   \item \code{SAILR.NO.WARN}: Determines whether warnings will be logged to the console
#'   \item \code{SAILR.THROW.ERRORS}: Specifies whether the current thread should be halted when an error is encountered; you are expected to wrap your \code{SAILR::Connection} calls with an error handler if you deactivate this option
#' }
#'
#' @import R6
#' @import odbc
#' @import DBI
#' @import stringr
#' @import rlang
#' @import vctrs
#' @import rstudioapi
#' @import data.table
#' @import microbenchmark
#'
#' @export
#'
Connection <- R6::R6Class(
  'Connection',
  public = list(
    #' @title Connection
    #'
    #' @description
    #' Initialise a new connection
    #'
    #' @param username (\code{character})\cr
    #'   An optional username; defaults to the local machine's username if \code{use.profile} is flagged, otherwise prompts the client to enter their username
    #' @param password (\code{character})\cr
    #'   An optional password
    #' @param database (\code{character})\cr
    #'   An optional database name; defaults to \code{SAILR.DEF$DATABASE} constant
    #' @param use.profile (\code{logical})\cr
    #'   Specifies whether SAILR should attempt to grab this database's password from the profile's keychain; defaults to \code{option(SAILR.USE.PROFILE=TRUE)}
    #'
    #'   Note: you will be prompted to insert your username/password if we fail your user with any associated with a SAILR::Profile keychain. Similarly, if
    #'         the connection fails to authenticate you will be prompted to re-enter your username and password
    #'
    #' @return A new SAILDB connection
    #'
    initialize = function (
      username    = NA,
      password    = NA,
      database    = SAILR.DEF$DATABASE,
      use.profile = getOption('SAILR.USE.SECRETS', TRUE)
    ) {
      use.profile = coerce.boolean(use.profile, default=TRUE)
      private$using.profile = use.profile

      private$profile = Profile$new()
      private$connect(username, password, database)
    },

    #' @title Connection$is.temporary
    #'
    #' @description
    #' Determines whether the given table is temporary or not based on its reference
    #'
    #' @param table.reference (\code{character})\cr
    #'   The table schema & name reference in the shape of \code{[SCHEMA].[TABLE]}
    #'
    #' @return Either (a) a logical reflecting the table's expected temporary status; or (b) an \code{NA} value for an invalid table reference
    #'
    is.temporary = function (table.reference) {
      ref = get.table.reference(table.reference)
      if (!is.list(ref)) {
        return (NA)
      }

      return (ref$schema == 'SESSION')
    },

    #' @title Connection$exists
    #'
    #' @description
    #' Determines whether a table exists
    #'
    #' @param table.reference (\code{character})\cr
    #'   The table schema & name reference in the shape of \code{[SCHEMA].[TABLE]}
    #' @param stop.on.error (\code{logical})\cr
    #'   Whether to return a \code{FALSE} logical when an error is encountered instead of stopping the execution of the parent thread; defaults to \code{option(SAILR.THROW.ERRORS=TRUE)}
    #' @param suppress.logs (\code{logical})\cr
    #'   Whether to suppress message logs; defaults to \code{option(SAILR.QUIET=FALSE)}
    #' @param suppress.warnings (\code{logical})\cr
    #'   Whether to suppress warnings; defaults to \code{option(SAILR.NO.WARN=FALSE)}
    #'
    #' @return Either (a) a logical reflecting a table's existence; or (b) an \code{NA} value for an invalid operation
    #'
    exists = function (
      table.reference   = NA,
      stop.on.error     = getOption('SAILR.THROW.ERRORS', TRUE),
      suppress.logs     = getOption('SAILR.QUIET', FALSE),
      suppress.warnings = getOption('SAILR.NO.WARN', FALSE)
    ) {
      caller.env = rlang::caller_env(1)
      time.called = microbenchmark::get_nanotime()

      ref = get.table.reference(table.reference)
      if (!is.list(ref)) {
        reason = attr(ref, 'reason')
        if (!rlang::is_scalar_character(reason)) {
          reason = SAILR.MSGS$NAMEERR
        }

        return (try.abort(stringr::str_interp('Invalid table reference with validation error: ${reason}'), call=caller.env, stop.on.error=stop.on.error))
      }

      username = self$username
      if (is.na(username) || !self$connected) {
        return (try.abort('Unable to determine table existence: no active, valid database connection found', call=caller.env, stop.on.error=stop.on.error))
      }

      try.log(
        'info', stringr::str_interp('[SAILR::Connection::Exists] Sending statement @ Time<${format(Sys.time())}>\n'),
        caller.env, suppress.logs, suppress.warnings
      )

      hnd = private$hnd
      res = tryCatch(
        {
          if (ref$schema == 'SESSION') {
            ## We'll try to read the session table catalogue but it's quite possible that the user doesn't
            ## have the privileges to do this so if we're unable to do so we'll fall back to a simple select
            res <- try(
              suppressWarnings(DBI::dbGetQuery(hnd, stringr::str_interp("
                SELECT 1
                  FROM TABLE (SYSPROC.admin_get_temp_tables(SYSPROC.mon_get_application_handle(), '', '')) GTT,
                      SYSCAT.TABLESPACES TBSPACE
                  WHERE TABNAME = '${toupper(ref$table)}'
                    AND INSTANTIATOR = '${toupper(username)}'
                    AND GTT.TBSP_ID = TBSPACE.TBSPACEID
                  LIMIT 1;
              "))),
              silent=TRUE
            )

            ## Attempt to select instead as we've failed to execute the query
            if (!is.data.frame(res)) {
              res <- try(
                suppressWarnings(DBI::dbGetQuery(hnd, stringr::str_interp("
                  SELECT 1
                    FROM ${table.reference}
                   LIMIT 1;
                "))),
                silent=TRUE
              )
            }

            res <- is.data.frame(res) && nrow(res) > 0
          } else {
            ## Check table catalogue for global tables
            res <- suppressWarnings(DBI::dbExistsTable(hnd, do.call(DBI::Id, ref)))
          }
        },
        error = function () {
          res <- FALSE
        }
      )

      time.called = round((microbenchmark::get_nanotime() - time.called)*1e-9, 4)
      try.log(
        'info', stringr::str_interp('[SAILR::Connection::Exists] Statement processed | Elapsed time: ${time.called}s\n'),
        caller.env, suppress.logs, suppress.warnings
      )

      return (res)
    },

    #' @title Connection$run
    #'
    #' @description
    #' Sends and executes an SQL statement; accepts and handles both \code{SELECT} queries, stored procedure execution, the creation of tables, and/or data manipulation queries
    #' like \code{INSERT} or \code{UPDATE}
    #'
    #' @param stmt (\code{character})\cr
    #'   The SQL statement string
    #' @param stop.on.error (\code{logical})\cr
    #'   Whether to return a \code{FALSE} logical when an error is encountered instead of stopping the execution of the parent thread; defaults to \code{option(SAILR.THROW.ERRORS=TRUE)}
    #' @param suppress.logs (\code{logical})\cr
    #'   Whether to suppress message logs; defaults to \code{option(SAILR.QUIET=FALSE)}
    #' @param suppress.warnings (\code{logical})\cr
    #'   Whether to suppress warnings; defaults to \code{option(SAILR.NO.WARN=FALSE)}
    #'
    #' @return Either:
    #'   \enumerate{
    #'     \item The resulting data.frame if executing a query \emph{e.g.} a \code{SELECT} statement
    #'     \item OR; a logical value reflecting the success if executing a statement \emph{e.g.} an \code{INSERT} statement, or a function call \emph{etc};
    #;      statements that affect rows may include a \code{rows.affected} attribute describing the number of rows affected
    #'   }
    #'
    run = function (
      stmt              = NA,
      stop.on.error     = getOption('SAILR.THROW.ERRORS', TRUE),
      suppress.logs     = getOption('SAILR.QUIET', FALSE),
      suppress.warnings = getOption('SAILR.NO.WARN', FALSE)
    ) {
      caller.env = rlang::caller_env(1)
      time.called = microbenchmark::get_nanotime()
      suppress.logs = coerce.boolean(suppress.logs, default=FALSE)
      suppress.warnings = coerce.boolean(suppress.warnings, default=FALSE)

      if (!self$connected) {
        return (try.abort('Unable to execute statement: no active, valid database connection found', call=caller.env, stop.on.error=stop.on.error))
      }

      if (!rlang::is_scalar_character(stmt)) {
        return (try.abort(stringr::str_interp('Expected Parameter<stmt> as scalar character but got ${class(stmt)[[1]]}'), call=caller.env, stop.on.error=stop.on.error))
      }

      if (is.string.empty(stmt)) {
        return (try.abort(stringr::str_interp('Expected Parameter<stmt> as non-empty character but got length(${nchar(trimws(stmt))})'), call=caller.env, stop.on.error=stop.on.error))
      }

      try.log(
        'info', stringr::str_interp('[SAILR::Connection::Run] Sending statement @ Time<${format(Sys.time())}>:\n\n${stmt}\n'),
        caller.env, suppress.logs, suppress.warnings
      )

      fnv = environment()
      hnd = private$hnd
      res = tryCatch(
        {
          stmt.result = NA

          on.exit({
            if (isS4(stmt.result) & inherits(stmt.result, 'DBIResult')) {
              DBI::dbClearResult(stmt.result)
            }
          })

          base::withCallingHandlers(
            {
              stmt.result = DBI::dbSendQuery(hnd, stmt)
              stmt.info = DBI::dbGetInfo(stmt.result)

              has.completed = coerce.boolean(stmt.info$has.completed, default=TRUE)
              if (!has.completed) {
                fnv$result.set = DBI::dbFetch(stmt.result, n=Inf)
              } else {
                fnv$rows.affected = ifelse(rlang::is_scalar_integerish(stmt.info$rows.affected), stmt.info$rows.affected, 0L)
              }

              (TRUE)
            },
            warning = function (w) {
              if (suppress.warnings) {
                return (invisible(NULL))
              } else if (!exists('wcondition')) {
                fnv$wcondition = c(wcondition)
              } else {
                fnv$wcondition = c(fnv$wcondition, w)
              }
              return (invisible(NULL))
            },
            error = function (e) (e)
          )
        },
        error = function (e) {
          fnv$err = TRUE
          conditionMessage(e)
        }
      )

      time.called = round((microbenchmark::get_nanotime() - time.called)*1e-9, 4)
      try.log(
        'info', stringr::str_interp('[SAILR::Connection::Run] Statement processed | Elapsed time: ${time.called}s\n'),
        caller.env, suppress.logs, suppress.warnings
      )

      if (!suppress.warnings && exists('wcondition')) {
        wmessage = paste0(as.character(seq_along(wcondition)), '.\t', unlist(as.character(wcondition), use.names=FALSE), collapse='\n')
        rlang::warn(paste('Statement experienced the following warnings:', wmessage, sep='\n\n'), call=caller.env)
      }

      has.error = exists('err')
      has.results = exists('result.set')
      has.effects = exists('rows.affected')
      if (has.error) {
        if (rlang::is_character(res) & !rlang::is_scalar_character(res)) {
          res = res[[1]]
        } else if (!rlang::is_scalar_character(res)) {
          res = SAILR.MSGS$UNKNOWN
        }

        return (try.abort(stringr::str_interp('Failed to execute statement:\n\n1. Error:\n${res}\n\n2. Failed statement:\n${stmt}'), call=caller.env, stop.on.error=stop.on.error))
      } else if (has.effects & !has.results) {
        attr(res, 'rows.affected') = rows.affected
        return (res)
      }

      return (result.set)
    },

    #' @title Connection$query
    #'
    #' @description
    #' Submits and synchronously executes the specified SQL query
    #'
    #' @param stmt (\code{character})\cr
    #'   The SQL statement string
    #' @param stop.on.error (\code{logical})\cr
    #'   Whether to return a \code{FALSE} logical when an error is encountered instead of stopping the execution of the parent thread; defaults to \code{option(SAILR.THROW.ERRORS=TRUE)}
    #' @param suppress.logs (\code{logical})\cr
    #'   Whether to suppress message logs; defaults to \code{option(SAILR.QUIET=FALSE)}
    #' @param suppress.warnings (\code{logical})\cr
    #'   Whether to suppress warnings; defaults to \code{option(SAILR.NO.WARN=FALSE)}
    #'
    #' @return The resulting data.frame from a \code{SELECT} statement; can return a logical if \code{stop.on.error} behaviour is inactive
    #'
    query = function (
      stmt               = NA,
      stop.on.error      = getOption('SAILR.THROW.ERRORS', TRUE),
      suppress.logs      = getOption('SAILR.QUIET', FALSE),
      suppress.warnings  = getOption('SAILR.NO.WARN', FALSE)
    ) {
      caller.env = rlang::caller_env(1)
      time.called = microbenchmark::get_nanotime()

      if (!self$connected) {
        return (try.abort('Unable to execute statement: no active, valid database connection found', call=caller.env, stop.on.error=stop.on.error))
      }

      if (!rlang::is_scalar_character(stmt)) {
        return (try.abort(stringr::str_interp('Expected Parameter<stmt> as scalar character but got ${class(stmt)[[1]]}'), call=caller.env, stop.on.error=stop.on.error))
      }

      if (is.string.empty(stmt)) {
        return (try.abort(stringr::str_interp('Expected Parameter<stmt> as non-empty character but got length(${nchar(trimws(stmt))})'), call=caller.env, stop.on.error=stop.on.error))
      }

      try.log(
        'info', stringr::str_interp('[SAILR::Connection::Query] Sending statement @ Time<${format(Sys.time())}>:\n\nn${stmt}\n'),
        caller.env, suppress.logs, suppress.warnings
      )

      hnd = private$hnd
      res = try(
        suppressWarnings(DBI::dbGetQuery(hnd, stmt)),
        silent=TRUE
      )

      time.called = round((microbenchmark::get_nanotime() - time.called)*1e-9, 4)
      try.log(
        'info', stringr::str_interp('[SAILR::Connection::Query] Statement processed | Elapsed time: ${time.called}s\n'),
        caller.env, suppress.logs, suppress.warnings
      )

      if (!is.data.frame(res)) {
        if (rlang::is_character(res) & !rlang::is_scalar_character(res)) {
          res = res[[1]]
        } else if (!rlang::is_scalar_character(res)) {
          res = SAILR.MSGS$UNKNOWN
        }

        return (try.abort(stringr::str_interp('Failed to execute query:\n\n1. Error:\n${res}\n\n2. Failed statement:\n${stmt}'), call=caller.env, stop.on.error=stop.on.error))
      }

      return (res)
    },

    #' @title Connection$execute
    #'
    #' @description
    #' Submits and synchronously executes an SQL statement
    #'
    #' @param stmt (\code{character})\cr
    #'   The SQL statement string
    #' @param stop.on.error (\code{logical})\cr
    #'   Whether to return a \code{FALSE} logical when an error is encountered instead of stopping the execution of the parent thread; defaults to \code{option(SAILR.THROW.ERRORS=TRUE)}
    #' @param suppress.logs (\code{logical})\cr
    #'   Whether to suppress message logs; defaults to \code{option(SAILR.QUIET=FALSE)}
    #' @param suppress.warnings (\code{logical})\cr
    #'   Whether to suppress warnings; defaults to \code{option(SAILR.NO.WARN=FALSE)}
    #'
    #' @return The number of rows affected; can return a logical if \code{stop.on.error} behaviour is inactive
    #'
    execute = function (
      stmt               = NA,
      stop.on.error      = getOption('SAILR.THROW.ERRORS', TRUE),
      suppress.logs      = getOption('SAILR.QUIET', FALSE),
      suppress.warnings  = getOption('SAILR.NO.WARN', FALSE)
    ) {
      caller.env = rlang::caller_env(1)
      time.called = microbenchmark::get_nanotime()

      if (!self$connected) {
        return (try.abort('Unable to execute statement: no active, valid database connection found', call=caller.env, stop.on.error=stop.on.error))
      }

      if (!rlang::is_scalar_character(stmt)) {
        return (try.abort(stringr::str_interp('Expected Parameter<stmt> as scalar character but got ${class(stmt)[[1]]}'), call=caller.env, stop.on.error=stop.on.error))
      }

      if (is.string.empty(stmt)) {
        return (try.abort(stringr::str_interp('Expected Parameter<stmt> as non-empty character but got length(${nchar(trimws(stmt))})'), call=caller.env, stop.on.error=stop.on.error))
      }

      try.log(
        'info', stringr::str_interp('[SAILR::Connection::Execute] Sending statement @ Time<${format(Sys.time())}>:\n\n${stmt}\n'),
        caller.env, suppress.logs, suppress.warnings
      )

      hnd = private$hnd
      res = try(
        suppressWarnings(DBI::dbExecute(hnd, stmt)),
        silent=TRUE
      )

      time.called = round((microbenchmark::get_nanotime() - time.called)*1e-9, 4)
      try.log(
        'info', stringr::str_interp('[SAILR::Connection::Execute] Statement processed | Elapsed time: ${time.called}s\n'),
        caller.env, suppress.logs, suppress.warnings
      )

      if (!rlang::is_scalar_integerish(res)) {
        if (rlang::is_character(res) & !rlang::is_scalar_character(res)) {
          res = res[[1]]
        } else if (!rlang::is_scalar_character(res)) {
          res = SAILR.MSGS$UNKNOWN
        }

        return (try.abort(stringr::str_interp('Failed to execute statement:\n\n1. Error:\n${res}\n\n2. Failed statement:\n${stmt}'), call=caller.env, stop.on.error=stop.on.error))
      }

      return (res)
    },

    #' @title Connection$transaction
    #'
    #' @description
    #' Run sequential, synchronous \code{SALIR::Connection} method calls as a transation; rollbacks can be performed by exiting the transaction using the \code{SALIR::Connection$exit.transaction}.
    #' Transactions are automatically committed at the end of the expression assuming no errors have occurred and/or no calls to \code{SALIR::Connection$exit.transaction} have
    #' taken place
    #'
    #' @param expr (\code{expression})\cr
    #'   Some arbitrary transaction expression
    #' @param stop.on.error (\code{logical})\cr
    #'   Whether to return a \code{FALSE} logical when an error is encountered instead of stopping the execution of the parent thread; defaults to \code{option(SAILR.THROW.ERRORS=TRUE)}
    #' @param suppress.logs (\code{logical})\cr
    #'   Whether to suppress message logs; defaults to \code{option(SAILR.QUIET=FALSE)}
    #' @param suppress.warnings (\code{logical})\cr
    #'   Whether to suppress warnings; defaults to \code{option(SAILR.NO.WARN=FALSE)}
    #'
    #' @return A logical reflecting the success of the transaction
    #'
    transaction = function (
      expr               = NA,
      stop.on.error      = getOption('SAILR.THROW.ERRORS', TRUE),
      suppress.logs      = getOption('SAILR.QUIET', FALSE),
      suppress.warnings  = getOption('SAILR.NO.WARN', FALSE)
    ) {
      caller.env = rlang::caller_env(1)
      time.called = microbenchmark::get_nanotime()

      if (!self$connected) {
        return (try.abort('Unable to execute statement: no active, valid database connection found', call=caller.env, stop.on.error=stop.on.error))
      }

      try.log(
        'info', stringr::str_interp('[SAILR::Connection::Transaction] Starting transaction @ Time<${format(Sys.time())}>\n'),
        caller.env, suppress.logs, suppress.warnings
      )

      hnd = private$hnd
      res = try(
        suppressWarnings(DBI::dbWithTransaction(hnd, expr)),
        silent=TRUE
      )

      time.called = round((microbenchmark::get_nanotime() - time.called)*1e-9, 4)
      try.log(
        'info', stringr::str_interp('[SAILR::Connection::Transaction] Statement(s) processed | Elapsed time: ${time.called}s\n'),
        caller.env, suppress.logs, suppress.warnings
      )

      if (rlang::is_character(res)) {
        if (rlang::is_character(res) & !rlang::is_scalar_character(res)) {
          res = res[[1]]
        } else if (!rlang::is_scalar_character(res)) {
          res = SAILR.MSGS$UNKNOWN
        }

        return (try.abort(stringr::str_interp('Failed to execute transaction:\n\n1. Error:\n${res}\n\n2. Failed expression:\n${expr}'), call=caller.env, stop.on.error=stop.on.error))
      }

      return (TRUE)
    },

    #' @title Connection$exit.transaction
    #'
    #' @description
    #' Used to exit a transaction and to perform a rollback from within a \code{SAILR::Connection$transaction} expression
    #'
    exit.transaction = function () {
      return (DBI::dbBreak())
    },

    #' @title Connection$save
    #'
    #' @description
    #' Attempt to save a data.frame object to a table; either by creating a new table or appending it to an existing table; note that this operation is transactional, so the statements will
    #' be rolled back if it fails at any point
    #'
    #' @param table.reference (\code{character})\cr
    #'   The table schema & name reference in the shape of \code{[SCHEMA].[TABLE]}
    #' @param table.data (\code{data.frame})\cr
    #'   The data.frame you wish to save
    #' @param can.append (\code{logical})\cr
    #'   Whether to append to the table if it already exists; defaults to \code{FALSE}
    #' @param can.overwrite (\code{logical})\cr
    #'   Whether to truncate and overwrite the table if it already exists; defaults to \code{FALSE}
    #' @param sanitise.columns (\code{logical})\cr
    #'   Whether to strip all non-DB2 compliant characters from a column's name; defaults to \code{TRUE}
    #' @param logical.as.integer (\code{logical})\cr
    #'   Whether to insert logical types as a \code{SMALLINT} datatype; otherwise resolves to a \code{VARCHAR} type; defaults to \code{TRUE}
    #' @param parse.datetimes (\code{logical})\cr
    #'   Whether to attempt to parse character columns as one of \code{[ DATE | TIME | TIMESTAMP ]}; defaults to \code{FALSE}
    #' @param reduce.db.logging (\code{logical})\cr
    #'   Alters global tables to stop initial logging; defaults to \code{TRUE}
    #' @param chunk.size (\code{integer})\cr
    #'   Determines the size of insert statement chunks; defaults to \code{option(SAILR.CHUNK.SIZE=SAILR.DEF$MIN.CHUNK.SIZE)}
    #' @param stop.on.error (\code{logical})\cr
    #'   Whether to return a \code{FALSE} logical when an error is encountered instead of stopping the execution of the parent thread; defaults to \code{option(SAILR.THROW.ERRORS=TRUE)}
    #' @param suppress.logs (\code{logical})\cr
    #'   Whether to suppress message logs; defaults to \code{option(SAILR.QUIET=FALSE)}
    #' @param suppress.warnings (\code{logical})\cr
    #'   Whether to suppress warnings; defaults to \code{option(SAILR.NO.WARN=FALSE)}
    #'
    #' @return A logical describing the success of the action
    #'
    save = function (
      table.reference    = NA,
      table.data         = NA,
      can.append         = FALSE,
      can.overwrite      = FALSE,
      sanitise.columns   = TRUE,
      logical.as.integer = TRUE,
      parse.datetimes    = FALSE,
      reduce.db.logging  = TRUE,
      chunk.size         = getOption('SAILR.CHUNK.SIZE', SAILR.DEF$MIN.CHUNK.SIZE),
      stop.on.error      = getOption('SAILR.THROW.ERRORS', TRUE),
      suppress.logs      = getOption('SAILR.QUIET', FALSE),
      suppress.warnings  = getOption('SAILR.NO.WARN', FALSE)
    ) {
      # Def. call stack
      caller.env = rlang::caller_env(1)
      time.called = microbenchmark::get_nanotime()

      # Validate parameters
      ref = get.table.reference(table.reference)
      if (!is.list(ref)) {
        reason = attr(ref, 'reason')
        if (!rlang::is_scalar_character(reason)) {
          reason = SAILR.MSGS$NAMEERR
        }

        return (try.abort(stringr::str_interp('Invalid table reference with validation error: ${reason}'), call=caller.env, stop.on.error=stop.on.error))
      }

      if (!self$connected) {
        return (try.abort('Unable to save data.frame to table: no active, valid database connection found', call=caller.env, stop.on.error=stop.on.error))
      }

      if (!is.data.frame(table.data)) {
        return (try.abort(stringr::str_interp('Expected data.frame but got ${class(table.data)[1]}'), call=caller.env, stop.on.error=stop.on.error))
      }

      # Parse options
      can.append = coerce.boolean(can.append, default=FALSE)
      can.overwrite = coerce.boolean(can.overwrite, default=FALSE)
      sanitise.columns = coerce.boolean(sanitise.columns, default=TRUE)
      logical.as.integer = coerce.boolean(logical.as.integer, default=TRUE)
      parse.datetimes = coerce.boolean(parse.datetimes, default=FALSE)
      reduce.db.logging = coerce.boolean(reduce.db.logging, default=TRUE)

      # Manage cases in which the table already exists
      ## Det. whether the table already exists
      build.table = TRUE
      table.exists = self$exists(table.reference, stop.on.error, suppress.logs, suppress.warnings)

      ## In cases where the user has opted not to stop execution when errors are thrown we should early exit
      if (is.na(table.exists)) {
        return (table.exists)
      }

      if (table.exists) {
        if (!can.append && !can.overwrite) {
          ## Try to throw an error if the user hasn't told us how to handle this case
          return (try.abort(
            stringr::str_interp('Table of name \'${table.reference}\' already exists but you have specified not to overwrite nor to append the given data.frame'),
            call=caller.env, stop.on.error=stop.on.error
          ))
        } else if (can.overwrite) {
          ## Attempt to drop the table
          success = self$drop(table.reference, stop.on.error=stop.on.error, suppress.logs=suppress.logs, suppress.warnings=suppress.warnings)

          ## Early exit in the cases where users have specified not to stop the thread on error
          if (!success) {
            return (success)
          }
        } else {
          ## Flag the \code{CREATE} step as unnecessary because we're going to be appending the data
          build.table = FALSE
        }
      }

      # Forward decl.
      ## Prepare statements
      is.temporary = ref$schema == 'SESSION'
      create.stmt = list()
      insert.prefix = paste0('INSERT INTO ', table.reference, ' VALUES')

      ## Compute table shape
      table.width = ncol(table.data)
      table.height = nrow(table.data)

      # Sanitise the input
      ## Coerce factors into strings
      column.names = sapply(table.data, is.factor)
      table.data[column.names] = lapply(table.data[column.names], as.character)

      ## Sanitise our column names if specified
      if (sanitise.columns) {
        colnames(table.data) = lapply(colnames(table.data), sanitise.string.name)
      }

      # Derive table structure
      ## Compute table shape
      column.names = colnames(table.data)
      column.final = tail(column.names, 1)

      ## Derive type from class & column values
      column.types = sapply(column.names, function (field.name) {
        field.elem = table.data[,field.name]
        field.type = class(field.elem)

        try.log(
          'info', stringr::str_interp('[SAILR::Connection::Save] Deriving struct of { column: ${field.name}, type: ${field.type[[1]]} }\n'),
          caller.env, suppress.logs, suppress.warnings
        )

        if (field.type == 'Date') {
          field.type = 'DATE'
          table.data[,field.name] <<- DBI::dbQuoteString(DBI::ANSI(), as.character(sapply(field.elem, function (x) {
            if (!is(x, 'ITime')) {
              return (NA)
            }

            return (format(x, '%Y-%m-%d'))
          })))
        } else if (field.type == 'POSIXct') {
          field.type = 'TIMESTAMP'
          table.data[,field.name] <<- DBI::dbQuoteString(DBI::ANSI(), as.character(sapply(field.elem, function (x) {
            if (!is(x, 'ITime')) {
              return (NA)
            }

            return (format(x, '%Y-%m-%d-%H.%M.%OS6'))
          })))
        } else if (field.type == 'ITime') {
          field.type = 'TIME'
          table.data[,field.name] <<- DBI::dbQuoteString(DBI::ANSI(), as.character(sapply(field.elem, function (x) {
            if (!is(x, 'ITime')) {
              return (NA)
            }

            return (format(x, '%H:%M:%OS'))
          })))
        } else if (field.type %in% list('integer64', 'integer', 'double', 'numeric')) {
          like.integer = rlang::is_integerish(field.elem)
          if (like.integer) {
            field.type = 'BIGINT'
            table.data[,field.name] <<- vctrs::vec_cast(field.elem, integer())
          } else {
            field.type = 'DOUBLE'
            table.data[,field.name] <<- vctrs::vec_cast(field.elem, double())
          }
        } else if (field.type == 'logical') {
          if (logical.as.integer) {
            field.type = 'SMALLINT'
            table.data[,field.name] <<- vctrs::vec_cast(field.elem, integer())
          } else {
            field.type = 'VARCHAR(6)'
            table.data[,field.name] <<- DBI::dbQuoteString(DBI::ANSI(), as.character(field.elem))
          }
        } else if (field.type == 'character') {
          alternate = NA
          if (parse.datetimes) {
            for (i in 1:table.height) {
              alternate = try(suppressWarnings(get.datetime.from.string(table.data[i,field.name])), silent=TRUE)
              if (!is.na(alternate)) {
                break
              }
            }
          }

          if (!is.na(alternate)) {
            alternate.type = class(alternate)[1]
            alternate.profile = SAILR.DEF$DATETIME.PROFILE[[alternate.type]]

            field.type = alternate.profile$datatype
            table.data[,field.name] <<- sapply(field.elem, function (x) {
              x = try(suppressWarnings(get.datetime.from.string(x)), silent=TRUE)
              if (is.na(x) || class(x)[1] != alternate.type) {
                return (NA)
              }

              return (mark.safe.value(format(x, alternate.profile$format)))
            })
          } else {
            field.type = paste0('VARCHAR(', toString(pmax(max(nchar(field.elem)), 1)), ')')
            table.data[,field.name] <<- DBI::dbQuoteString(DBI::ANSI(), as.character(field.elem))
          }
        } else {
          field.type = 'VARCHAR(10000)'
          table.data[,field.name] <<- DBI::dbQuoteString(DBI::ANSI(), as.character(field.elem))
        }

        if (build.table) {
          stmt.suffix = ifelse(column.final == field.name, '', ',')
          create.stmt <<- c(create.stmt, paste('', field.name, paste0(field.type, stmt.suffix), sep='\t'))
        }

        return (field.type)
      })

      ## Mark null values as SQL safe value (will be escaped when concatenated)
      table.data[is.na(table.data) | is.null(table.data)] = 'NULL'

      # Start transaction
      hnd = private$hnd
      err = NA
      res = try(
        suppressWarnings(DBI::dbWithTransaction(
          hnd,
          {
            ## Create table if applicable
            if (build.table) {
              ### Build \code{CREATE} statement
              create.stmt = do.call(paste, append(create.stmt, list(sep='\n')))
              if (is.temporary) {
                create.stmt = paste0(
                  'DECLARE GLOBAL TEMPORARY TABLE ', table.reference, ' (\n',
                    create.stmt, '\n',
                  ')\n',
                  '   WITH REPLACE\n',
                  '     ON COMMIT PRESERVE ROWS;'
                )
              } else {
                create.stmt = paste0(
                  'CREATE TABLE ', table.reference, ' (\n',
                    create.stmt, '\n',
                  ') NOT LOGGED INITIALLY;'
                )
              }

              try.log(
                'info', stringr::str_interp('[SAILR::Connection::Save] Sending statement @ Time<${format(Sys.time())}>:\n\n${create.stmt}\n'),
                caller.env, suppress.logs, suppress.warnings
              )

              ### Execute \code{CREATE} statement & handle response
              effect = try(
                suppressWarnings(DBI::dbExecute(hnd, create.stmt)),
                silent=TRUE
              )

              if (!rlang::is_scalar_integerish(effect)) {
                if (rlang::is_character(effect) & !rlang::is_scalar_character(effect)) {
                  effect <- effect[[1]]
                } else if (!rlang::is_scalar_character(effect)) {
                  effect <- SAILR.MSGS$UNKNOWN
                }

                err <- stringr::str_interp('Failed to create table \'${table.reference}\' with error:\n\n${effect}')
                DBI::dbBreak()
              }

              ### Reduce table logging if possible for this schema type
              if (reduce.db.logging && !is.temporary) {
                effect = try(
                  suppressWarnings(DBI::dbExecute(hnd, paste('ALTER TABLE', table.reference, 'ACTIVATE NOT LOGGED INITIALLY;', sep=' '))),
                  silent=TRUE
                )

                if (!rlang::is_scalar_integerish(effect)) {
                  if (rlang::is_character(effect) & !rlang::is_scalar_character(effect)) {
                    effect <- effect[[1]]
                  } else if (!rlang::is_scalar_character(effect)) {
                    effect <- SAILR.MSGS$UNKNOWN
                  }

                  err <- stringr::str_interp('Failed to alter table \'${table.reference}\' when attemtping to deactivate initial logging with error: ${effect}')
                  DBI::dbBreak()
                }
              }
            }

            ## Determine the best insert method for the given parameters
            ##
            ##  i.e. do we want chunked statements or a complete set?
            ##
            chunk.size = ifelse(rlang::is_bare_integerish(chunk.size), as.integer(chunk.size), -1L)
            chunk.groups = ((chunk.size >= 10) & ((table.height %/% chunk.size) > 1L))

            if (chunk.groups) {
              ### Build & execute statement chunks sequentially for temporary tables
              chunk.groups = rep(chunk.size, table.height %/% chunk.size)
              chunk.remainder = head(table.height %% chunk.groups, 1L)
              if (chunk.remainder != 0) {
                chunk.groups = c(chunk.groups, chunk.remainder)
              }

              for (pos in seq_along(chunk.groups)) {
                time.started = microbenchmark::get_nanotime()
                rown = seq_len(chunk.groups[pos]) + (pos - 1L)*chunk.size
                rows = table.data[rown,]

                stmt = apply(rows, 1, function (x) {
                  x = (do.call(paste, append(x, list(sep=','))))
                  return (paste0('(', x, ')'))
                })

                stmt = do.call(paste, append(stmt, list(sep=',\n\t')))
                stmt = paste(insert.prefix, paste0(stmt, ';'), sep='\n\t')

                #### Handle status of each
                time.insert = microbenchmark::get_nanotime()
                effect = try(
                  suppressWarnings(DBI::dbExecute(hnd, stmt)),
                  silent=TRUE
                )

                if (!rlang::is_scalar_integerish(effect)) {
                  if (rlang::is_character(effect) & !rlang::is_scalar_character(effect)) {
                    effect <- effect[[1]]
                  } else if (!rlang::is_scalar_character(effect)) {
                    effect <- SAILR.MSGS$UNKNOWN
                  }

                  err <- stringr::str_interp('Failed to execute insert statement on \'${table.reference}\' with err:\n\n${effect}')
                  DBI::dbBreak()
                }

                time.started = round((time.insert - time.started)*1e-9, 4)
                time.insert = round((microbenchmark::get_nanotime() - time.insert)*1e-9, 4)
                try.log(
                  'info', stringr::str_interp('[SAILR::Connection::Save] Rows Inserted: ${max(rown)} | Timings { Build: ${time.started}s, Insert: ${time.insert}s }\n'),
                  caller.env, suppress.logs, suppress.warnings
                )
              }
            } else {
              ### Since chunking isn't appropriate for smaller statements we will build
              ### the entire insert statement sequentially before executing it
              ###
              stmt = apply(table.data, 1, function (x) {
                x = (do.call(paste, append(x, list(sep=','))))
                return (paste0('(', x, ')'))
              })

              stmt = do.call(paste, append(stmt, list(sep=',\n\t')))
              stmt = paste(insert.prefix, paste0(stmt, ';'), sep='\n\t')

              try.log(
                'info', stringr::str_interp('[SAILR::Connection::Save] Sending statement @ Time<${format(Sys.time())}>:\n\n${stmt}\n'),
                caller.env, suppress.logs, suppress.warnings
              )

              ### Handle status
              effect = try(
                suppressWarnings(DBI::dbExecute(hnd, stmt)),
                silent=TRUE
              )

              if (!rlang::is_scalar_integerish(effect)) {
                if (rlang::is_character(effect) & !rlang::is_scalar_character(effect)) {
                  effect <- effect[[1]]
                } else if (!rlang::is_scalar_character(effect)) {
                  effect <- SAILR.MSGS$UNKNOWN
                }

                err <- stringr::str_interp('Failed to execute insert statement on \'${table.reference}\' with err:\n\n${effect}')
                DBI::dbBreak()
              }
            }

            (TRUE)
          }
        )),
        silent=TRUE
      )

      time.called = round((microbenchmark::get_nanotime() - time.called)*1e-9, 4)
      try.log(
        'info', stringr::str_interp('[SAILR::Connection::Save] Statement processed | Elapsed time: ${time.called}s\n'),
        caller.env, suppress.logs, suppress.warnings
      )

      if (!rlang::is_logical(err)) {
        msg = err
        if (rlang::is_character(msg) & !rlang::is_scalar_character(msg)) {
          msg <- effect[[1]]
        } else if (!rlang::is_scalar_character(msg)) {
          msg <- SAILR.MSGS$UNKNOWN
        }

        return (try.abort(stringr::str_interp('Failed to create table from data.frame with error:\n\n${msg}'), call=caller.env, stop.on.error=stop.on.error))
      }

      return (TRUE)
    },

    #' @title Connection$create.from
    #'
    #' @description
    #' Wrapper method to create a new table from a \code{SELECT} statement; note that this operation is transactional, so the statements will
    #' be rolled back if it fails at any point
    #'
    #' @param table.reference (\code{character})\cr
    #'   The table schema & name reference in the shape of \code{[SCHEMA].[TABLE]}
    #' @param stmt (\code{character})\cr
    #'   The select query used to build your cloned table
    #' @param can.append (\code{logical})\cr
    #'   Whether to append to the table if it already exists; defaults to \code{FALSE}
    #' @param can.overwrite (\code{logical})\cr
    #'   Whether to truncate and overwrite the table if it already exists; defaults to \code{FALSE}
    #' @param reduce.db.logging (\code{logical})\cr
    #'   Alters global tables to stop initial logging; defaults to \code{TRUE}
    #' @param stop.on.error (\code{logical})\cr
    #'   Whether to return a \code{FALSE} logical when an error is encountered instead of stopping the execution of the parent thread; defaults to \code{option(SAILR.THROW.ERRORS=TRUE)}
    #' @param suppress.logs (\code{logical})\cr
    #'   Whether to suppress message logs; defaults to \code{option(SAILR.QUIET=FALSE)}
    #' @param suppress.warnings (\code{logical})\cr
    #'   Whether to suppress warnings; defaults to \code{option(SAILR.NO.WARN=FALSE)}
    #'
    #' @return A logical describing the success of the action
    #'
    create.from = function (
      table.reference    = NA,
      stmt               = NA,
      can.append         = FALSE,
      can.overwrite      = FALSE,
      reduce.db.logging  = TRUE,
      stop.on.error      = getOption('SAILR.THROW.ERRORS', TRUE),
      suppress.logs      = getOption('SAILR.QUIET', FALSE),
      suppress.warnings  = getOption('SAILR.NO.WARN', FALSE)
    ) {
      # Def. call stack
      caller.env = rlang::caller_env(1)
      time.called = microbenchmark::get_nanotime()

      # Validate parameters
      ref = get.table.reference(table.reference)
      if (!is.list(ref)) {
        reason = attr(ref, 'reason')
        if (!rlang::is_scalar_character(reason)) {
          reason = SAILR.MSGS$NAMEERR
        }

        return (try.abort(stringr::str_interp('Invalid table reference with validation error: ${reason}'), call=caller.env, stop.on.error=stop.on.error))
      }

      if (!self$connected) {
        return (try.abort('Unable to save perform copy: no active, valid database connection found', call=caller.env, stop.on.error=stop.on.error))
      }

      if (is.string.empty(stmt)) {
        return (try.abort(stringr::str_interp('Expected Parameter<stmt> as non-empty character but got length(${nchar(trimws(stmt))})'), call=caller.env, stop.on.error=stop.on.error))
      }

      # Parse options
      can.append = coerce.boolean(can.append, default=FALSE)
      can.overwrite = coerce.boolean(can.overwrite, default=FALSE)
      reduce.db.logging = coerce.boolean(reduce.db.logging, default=TRUE)

      # Manage cases in which the table already exists
      ## Det. whether the table already exists
      build.table = TRUE
      is.temporary = ref$schema == 'SESSION'
      table.exists = self$exists(table.reference, stop.on.error, suppress.logs, suppress.warnings)

      ## In cases where the user has opted not to stop execution when errors are thrown we should early exit
      if (is.na(table.exists)) {
        return (table.exists)
      }

      if (table.exists) {
        if (!can.append && !can.overwrite) {
          ## Try to throw an error if the user hasn't told us how to handle this case
          return (try.abort(
            stringr::str_interp('Table of name \'${table.reference}\' already exists but you have specified not to overwrite nor to append the specified statement'),
            call=caller.env, stop.on.error=stop.on.error
          ))
        } else if (can.overwrite) {
          ## Attempt to drop the table
          success = self$drop(table.reference, stop.on.error=stop.on.error, suppress.logs=suppress.logs, suppress.warnings=suppress.warnings)

          ## Early exit in the cases where users have specified not to stop the thread on error
          if (!success) {
            return (success)
          }
        } else {
          ## Flag the \code{CREATE} step as unnecessary because we're going to be appending the data
          build.table = FALSE
        }
      }

      # Start transaction
      hnd = private$hnd
      err = NA
      res = try(
        suppressWarnings(DBI::dbWithTransaction(
          hnd,
          {
            ## Create table if applicable
            if (build.table) {
              ### Build \code{CREATE} statement
              create.stmt = ''
              if (is.temporary) {
                create.stmt = paste0(
                  'DECLARE GLOBAL TEMPORARY TABLE ', table.reference, ' AS (\n',
                    stmt, '\n',
                  ')\n',
                  '   WITH NO DATA\n',
                  '   WITH REPLACE\n',
                  '     ON COMMIT PRESERVE ROWS;'
                )
              } else {
                create.stmt = paste0(
                  'CREATE TABLE ', table.reference, ' AS (\n',
                    stmt, '\n',
                  ')',
                  '   WITH NO DATA;'
                )
              }

              ### Execute \code{CREATE} statement & handle response
              try.log(
                'info', stringr::str_interp('[SAILR::Connection::create.from] Sending statement @ Time<${format(Sys.time())}>:\n${create.stmt}\n'),
                caller.env, suppress.logs, suppress.warnings
              )

              effect = try(
                suppressWarnings(DBI::dbExecute(hnd, create.stmt)),
                silent=TRUE
              )

              if (!rlang::is_scalar_integerish(effect)) {
                if (rlang::is_character(effect) & !rlang::is_scalar_character(effect)) {
                  effect <- effect[[1]]
                } else if (!rlang::is_scalar_character(effect)) {
                  effect <- SAILR.MSGS$UNKNOWN
                }

                err <- stringr::str_interp('Failed to create table \'${table.reference}\' with error:\n\n${effect}')
                DBI::dbBreak()
              }

              ### Reduce table logging if possible for this schema type
              if (reduce.db.logging && !is.temporary) {
                effect = try(
                  suppressWarnings(DBI::dbExecute(hnd, paste('ALTER TABLE', table.reference, 'ACTIVATE NOT LOGGED INITIALLY;', sep=' '))),
                  silent=TRUE
                )

                if (!rlang::is_scalar_integerish(effect)) {
                  if (rlang::is_character(effect) & !rlang::is_scalar_character(effect)) {
                    effect <- effect[[1]]
                  } else if (!rlang::is_scalar_character(effect)) {
                    effect <- SAILR.MSGS$UNKNOWN
                  }

                  err <- stringr::str_interp('Failed to alter table \'${table.reference}\' when attemtping to deactivate initial logging with error:\n\n${effect}')
                  DBI::dbBreak()
                }
              }
            }

            ## Det. column info
            query.tbl = try(
              suppressWarnings(DBI::dbSendQuery(hnd, stringr::str_interp("
                SELECT *
                  FROM ${table.reference}
                LIMIT 1;
              "))),
              silent=TRUE
            )

            if (!isS4(query.tbl) | !inherits(query.tbl, 'DBIResult')) {
              msg = query.tbl
              if (rlang::is_character(msg) & !rlang::is_scalar_character(msg)) {
                msg <- msg[[1]]
              } else if (!rlang::is_scalar_character(msg)) {
                msg <- SAILR.MSGS$UNKNOWN
              }

              err <- stringr::str_interp('Failed to query table \'${table.reference}\', got error:\n\n${msg}')
              DBI::dbBreak()
            }

            column.info = try(
              suppressWarnings(DBI::dbColumnInfo(query.tbl)),
              silent=TRUE
            )

            if (!is.data.frame(column.info)) {
              msg = column.info
              if (rlang::is_character(msg) & !rlang::is_scalar_character(msg)) {
                msg <- msg[[1]]
              } else if (!rlang::is_scalar_character(msg)) {
                msg <- SAILR.MSGS$UNKNOWN
              }

              err <- stringr::str_interp('Failed to build column name list when attempting to copy to \'${table.reference}\', got error:\n\n${msg}')
              DBI::dbBreak()
            }

            ## Try insert
            column.info = do.call(paste, append(as.list(column.info[, 'name']), list(sep=', ')))
            insert.stmt = stringr::str_interp("
              INSERT INTO ${table.reference} (${column.info})
                ${stmt}
            ")

            try.log(
              'info', stringr::str_interp('[SAILR::Connection::create.from] Sending statement @ Time<${format(Sys.time())}>:\n${insert.stmt}\n'),
              caller.env, suppress.logs, suppress.warnings
            )

            effect = try(
              suppressWarnings(DBI::dbExecute(hnd, insert.stmt)),
              silent=TRUE
            )

            if (!rlang::is_scalar_integerish(effect)) {
              if (rlang::is_character(effect) & !rlang::is_scalar_character(effect)) {
                effect <- effect[[1]]
              } else if (!rlang::is_scalar_character(effect)) {
                effect <- SAILR.MSGS$UNKNOWN
              }

              err <- stringr::str_interp('Failed to exeecute insert statement for table \'${table.reference}\', got error:\n\n${effect}')
              DBI::dbBreak()
            }
          }
        )),
        silent=TRUE
      )

      time.called = round((microbenchmark::get_nanotime() - time.called)*1e-9, 4)
      try.log(
        'info', stringr::str_interp('[SAILR::Connection::create.from] Statement processed | Elapsed time: ${time.called}s\n'),
        caller.env, suppress.logs, suppress.warnings
      )

      if (!is.na(err)) {
        msg = err
        if (rlang::is_character(err) & !rlang::is_scalar_character(err)) {
          msg <- effect[[1]]
        } else if (!rlang::is_scalar_character(err)) {
          msg <- SAILR.MSGS$UNKNOWN
        }

        return (try.abort(stringr::str_interp('Failed to clone table from statement with error:\n\n${msg}'), call=caller.env, stop.on.error=stop.on.error))
      }

      return (TRUE)
    },

    #' @title Connection$map.from
    #'
    #' @description
    #' Wrapper method to create a new table from another table whilst mapping its columns to a different subset; note that this operation is transactional, so the statements will
    #' be rolled back if it fails at any point
    #'
    #' @param input.reference (\code{character})\cr
    #'   The input table to clone from in the shape of \code{[SCHEMA].[TABLE]}
    #' @param output.reference (\code{character})\cr
    #'   The output table to map into, in the shape of \code{[SCHEMA].[TABLE]}
    #' @param output.map (\code{list})\cr
    #'   A list of key-value pair characters describing the column name mapping and/or a subset of columns
    #' @param can.overwrite (\code{logical})\cr
    #'   Whether to truncate and overwrite the table if it already exists; defaults to \code{FALSE}
    #' @param reduce.db.logging (\code{logical})\cr
    #'   Alters global tables to stop initial logging; defaults to \code{TRUE}
    #' @param stop.on.error (\code{logical})\cr
    #'   Whether to return a \code{FALSE} logical when an error is encountered instead of stopping the execution of the parent thread; defaults to \code{option(SAILR.THROW.ERRORS=TRUE)}
    #' @param suppress.logs (\code{logical})\cr
    #'   Whether to suppress message logs; defaults to \code{option(SAILR.QUIET=FALSE)}
    #' @param suppress.warnings (\code{logical})\cr
    #'   Whether to suppress warnings; defaults to \code{option(SAILR.NO.WARN=FALSE)}
    #'
    #' @return A logical describing the success of the operation
    #'
    map.from = function (
      input.reference    = NA,
      output.reference   = NA,
      output.map         = NA,
      can.overwrite      = FALSE,
      reduce.db.logging  = TRUE,
      stop.on.error      = getOption('SAILR.THROW.ERRORS', TRUE),
      suppress.logs      = getOption('SAILR.QUIET', FALSE),
      suppress.warnings  = getOption('SAILR.NO.WARN', FALSE)
    ) {
      # Def. call stack
      caller.env = rlang::caller_env(1)
      time.called = microbenchmark::get_nanotime()

      # Validate parameters
      input.ref = get.table.reference(input.reference)
      if (!is.list(input.ref)) {
        reason = attr(input.ref, 'reason')
        if (!rlang::is_scalar_character(reason)) {
          reason = SAILR.MSGS$NAMEERR
        }

        return (try.abort(stringr::str_interp('Invalid input table reference with validation error: ${reason}'), call=caller.env, stop.on.error=stop.on.error))
      }

      output.ref = get.table.reference(output.reference)
      if (!is.list(output.ref)) {
        reason = attr(output.ref, 'reason')
        if (!rlang::is_scalar_character(reason)) {
          reason = SAILR.MSGS$NAMEERR
        }

        return (try.abort(stringr::str_interp('Invalid input table reference with validation error: ${reason}'), call=caller.env, stop.on.error=stop.on.error))
      }

      is.valid.map = (
        rlang::is_list(output.map)
        && rlang::is_character(unlist(output.map, use.names=FALSE))
        && length(output.map) == sum(names(output.map) != '', na.rm=TRUE)
      )

      if (!is.valid.map) {
        return (try.abort('Expected a character key-value pair list in which all elements are named', call=caller.env, stop.on.error=stop.on.error))
      }

      if (!self$connected) {
        return (try.abort('Unable to save perform mapping: no active, valid database connection found', call=caller.env, stop.on.error=stop.on.error))
      }

      # Parse options
      can.overwrite = coerce.boolean(can.overwrite, default=FALSE)
      reduce.db.logging = coerce.boolean(reduce.db.logging, default=TRUE)

      # Manage cases in which the input table doesn't exist
      ## Det. whether the table exists
      table.exists = self$exists(input.reference, stop.on.error, suppress.logs, suppress.warnings)

      ## In cases where the user has opted not to stop execution when errors are thrown we should early exit
      if (is.na(table.exists)) {
        return (table.exists)
      }

      ## Attempt to abort if the input table doesn't exist
      if (!table.exists) {
        return (try.abort(
          stringr::str_interp('Table of name \'${input.reference}\' doesn\'t exist'),
          call=caller.env, stop.on.error=stop.on.error
        ))
      }

      # Manage cases in which the output table already exists
      ## Det. whether the table already exists
      is.temporary = output.ref$schema == 'SESSION'
      table.exists = self$exists(output.reference, stop.on.error, suppress.logs, suppress.warnings)

      ## In cases where the user has opted not to stop execution when errors are thrown we should early exit
      if (is.na(table.exists)) {
        return (table.exists)
      }

      ## Manage output existence...
      if (table.exists && !can.overwrite) {
        ### Try to throw an error if the user hasn't told us how to handle this case
        return (try.abort(
          stringr::str_interp('Table of name \'${output.reference}\' already exists but you have specified not to overwrite the table'),
          call=caller.env, stop.on.error=stop.on.error
        ))
      } else if (can.overwrite) {
        ### Attempt to drop the table
        success = self$drop(output.reference, stop.on.error=stop.on.error, suppress.logs=suppress.logs, suppress.warnings=suppress.warnings)

        ### Early exit in the cases where users have specified not to stop the thread on error
        if (!success) {
          return (success)
        }
      }

      # Forward decl.
      delimiter = list(sep=',\n')
      to.names = unlist(output.map, use.names=FALSE)
      from.names = unlist(names(output.map), use.names=FALSE)

      # Start transaction
      hnd = private$hnd
      err = NA
      res = try(
        suppressWarnings(DBI::dbWithTransaction(
          hnd,
          {
            ## Build table representation
            ### Determine column shape
            is.temp = self$is.temporary(input.reference)
            if (!is.temp) {
              input.stmt = stringr::str_interp("
                SELECT
                    COLNAME,
                    CASE
                      WHEN TYPENAME = 'VARCHAR'   THEN   'VARCHAR(' || LENGTH || ')'
                      WHEN TYPENAME = 'CHARACTER' THEN 'CHARACTER(' || LENGTH || ')'
                      WHEN TYPENAME = 'DECIMAL'   THEN   'DECIMAL(' || LENGTH || ',' || SCALE || ')'
                      ELSE TYPENAME
                    END AS COLTYPE
                  FROM SYSCAT.COLUMNS
                WHERE TABSCHEMA = '${input.ref$schema}'
                  AND TABNAME = '${input.ref$table}';
              ")
            } else {
              input.stmt = stringr::str_interp("
                SELECT
                    COL.COLNAME,
                    CASE
                      WHEN COL.TYPENAME = 'VARCHAR'   THEN   'VARCHAR(' || COL.LENGTH || ')'
                      WHEN COL.TYPENAME = 'CHARACTER' THEN 'CHARACTER(' || COL.LENGTH || ')'
                      WHEN COL.TYPENAME = 'DECIMAL'   THEN   'DECIMAL(' || COL.LENGTH || ',' || COL.SCALE || ')'
                      ELSE COL.TYPENAME
                    END AS COLTYPE
                  FROM SYSIBMADM.ADMINTEMPCOLUMNS AS COL,
                       SYSIBMADM.ADMINTEMPTABLES AS TAB
                WHERE TAB.TEMPTABTYPE = 'D'
                  AND TAB.INSTANTIATOR = SYSTEM_USER
                  AND (TAB.TABSCHEMA = '${input.ref$schema}' AND TAB.TABNAME = '${input.ref$table}')
                  AND (COL.TABNAME = TAB.TABNAME AND COL.TABSCHEMA = TAB.TABSCHEMA);
              ")
            }

            try.log(
              'info', stringr::str_interp('[SAILR::Connection::map.from] Sending statement @ Time<${format(Sys.time())}>:\n${input.stmt}\n'),
              caller.env, suppress.logs, suppress.warnings
            )

            input.cols = try(
              suppressWarnings(DBI::dbGetQuery(hnd, input.stmt)),
              silent=TRUE
            )

            if (!is.data.frame(input.cols)) {
              msg = input.cols
              if (rlang::is_character(msg) & !rlang::is_scalar_character(msg)) {
                msg <- msg[[1]]
              } else if (!rlang::is_scalar_character(msg)) {
                msg <- SAILR.MSGS$UNKNOWN
              }

              err <- stringr::str_interp('Failed to derive input table shape from \'${input.reference}\', got error:\n\n${msg}')
              DBI::dbBreak()
            }

            ### Process columns
            input.cols$COLNAME = vctrs::vec_cast(input.cols$COLNAME, character())
            input.cols = input.cols[match(from.names, input.cols$COLNAME),]
            col.max.len = pmax(max(nchar(to.names)), max(nchar(input.cols$COLNAME)), 1)

            ## Generate statements
            insert.stmt = list()
            insert.into = list()

            create.stmt = apply(input.cols, 1, function (x) {
              col.name = x[[1]]
              col.type = x[[2]]

              col.target = col.name
              if (col.name %in% from.names) {
                col.target = output.map[[col.name]]
              }

              sep = col.max.len - nchar(col.target)
              if (sep < 1) {
                sep = ' '
              } else {
                sep = do.call(paste0, as.list(rep(' ', times=sep)))
              }

              insert.stmt <<- append(insert.stmt, stringr::str_interp("    \"${col.name}\" AS \"${col.target}\""))
              insert.into <<- append(insert.into, stringr::str_interp("  \"${col.target}\""))

              return (stringr::str_interp("  \"${col.target}\"${sep}${col.type}"))
            })

            ## Build & exec create statement
            create.stmt = do.call(paste, append(create.stmt, delimiter))
            if (!is.temp) {
              create.stmt = paste0(
                'CREATE TABLE ', output.reference, ' (\n',
                  create.stmt, '\n',
                ')'
              )
            } else {
              create.stmt = paste0(
                'DECLARE GLOBAL TEMPORARY TABLE ', output.reference, ' (\n',
                  create.stmt, '\n',
                ')\n',
                '  WITH REPLACE\n',
                '  ON COMMIT PRESERVE ROWS'
              )
            }

            if (!is.temp && reduce.db.logging) {
              create.stmt = paste0(create.stmt, '\n  NOT LOGGED INITIALLY;')
            } else {
              create.stmt = paste0(create.stmt, ';')
            }

            try.log(
              'info', stringr::str_interp('[SAILR::Connection::map.from] Sending statement @ Time<${format(Sys.time())}>:\n${create.stmt}\n'),
              caller.env, suppress.logs, suppress.warnings
            )

            effect = try(
              suppressWarnings(DBI::dbExecute(hnd, create.stmt)),
              silent=TRUE
            )

            if (!rlang::is_scalar_integerish(effect)) {
              if (rlang::is_character(effect) & !rlang::is_scalar_character(effect)) {
                effect <- effect[[1]]
              } else if (!rlang::is_scalar_character(effect)) {
                effect <- SAILR.MSGS$UNKNOWN
              }

              err <- stringr::str_interp('Failed to create table \'${output.reference}\' with error:\n\n${effect}')
              DBI::dbBreak()
            }

            ## Build & exec insert statement
            insert.into = do.call(paste, append(insert.into, delimiter))
            insert.stmt = do.call(paste, append(insert.stmt, delimiter))

            insert.stmt = paste0(
              'INSERT INTO ', output.reference, ' (\n',
                insert.into, '\n',
              ')\n',
              'SELECT', '\n',
                insert.stmt, '\n',
              '  FROM ', input.reference, ';'
            )

            try.log(
              'info', stringr::str_interp('[SAILR::Connection::map.from] Sending statement @ Time<${format(Sys.time())}>:\n${insert.stmt}\n'),
              caller.env, suppress.logs, suppress.warnings
            )

            effect = try(
              suppressWarnings(DBI::dbExecute(hnd, insert.stmt)),
              silent=TRUE
            )

            if (!rlang::is_scalar_integerish(effect)) {
              if (rlang::is_character(effect) & !rlang::is_scalar_character(effect)) {
                effect <- effect[[1]]
              } else if (!rlang::is_scalar_character(effect)) {
                effect <- SAILR.MSGS$UNKNOWN
              }

              err <- stringr::str_interp('Failed to insert rows into \'${output.reference}\' with error:\n\n${effect}')
              DBI::dbBreak()
            }
          }
        )),
        silent=TRUE
      )

      time.called = round((microbenchmark::get_nanotime() - time.called)*1e-9, 4)
      try.log(
        'info', stringr::str_interp('[SAILR::Connection::map.from] Statement processed | Elapsed time: ${time.called}s\n'),
        caller.env, suppress.logs, suppress.warnings
      )

      if (!is.na(err)) {
        msg = err
        if (rlang::is_character(err) & !rlang::is_scalar_character(err)) {
          msg <- effect[[1]]
        } else if (!rlang::is_scalar_character(err)) {
          msg <- SAILR.MSGS$UNKNOWN
        }

        return (try.abort(stringr::str_interp('Failed to map table with error:\n\n${msg}'), call=caller.env, stop.on.error=stop.on.error))
      }

      return (TRUE)
    },

    #' @title Connection$grant
    #'
    #' @description
    #' Attempts to grant \code{SELECT} on the specified schema, or tables, to the specified user(s)
    #'
    #' @param schema (\code{character})\cr
    #'   The schema name
    #' @param tables (`character|list<character>`)\cr
    #'   An optional list/vector of table names to grant access to; in the shape of either (a) a scalar character, (b) a character vector, or (c) a list of characters
    #' @param users (`character|list<character>`)\cr
    #'   A list of users to grant \code{SELECT} permission to
    #' @param stop.on.error (\code{logical})\cr
    #'   Whether to return a \code{FALSE} logical when an error is encountered instead of stopping the execution of the parent thread; defaults to \code{option(SAILR.THROW.ERRORS=TRUE)}
    #' @param suppress.logs (\code{logical})\cr
    #'   Whether to suppress message logs; defaults to \code{option(SAILR.QUIET=FALSE)}
    #' @param suppress.warnings (\code{logical})\cr
    #'   Whether to suppress warnings; defaults to \code{option(SAILR.NO.WARN=FALSE)}
    #'
    #' @return A logical that describes whether the action was successful
    #'
    grant = function (
      schema            = NA,
      tables            = NA,
      users             = NA,
      stop.on.error     = getOption('SAILR.THROW.ERRORS', TRUE),
      suppress.logs     = getOption('SAILR.QUIET', FALSE),
      suppress.warnings = getOption('SAILR.NO.WARN', FALSE)
    ) {
      caller.env = rlang::caller_env(1)
      time.called = microbenchmark::get_nanotime()

      if (!rlang::is_scalar_character(schema)) {
        return (try.abort(
          stringr::str_interp('Expected Parameter<schema> as a scalar character but got ${class(schema)[[1]]}'),
          call=caller.env, stop.on.error=stop.on.error
        ))
      }

      if (any((is.na(tables) | is.null(tables)) == TRUE) || (!rlang::is_character(tables) && !(is.list(tables) && any(lapply(tables, rlang::is_scalar_character) == FALSE)))) {
        return (try.abort(
          stringr::str_interp('Expected Parameter<tables> as either a scalar|vector character or a list of characters but got ${class(tables)[[1]]}'),
          call=caller.env, stop.on.error=stop.on.error
        ))
      }

      if (!rlang::is_character(users) && !(is.list(users) && any(lapply(users, rlang::is_scalar_character) == FALSE))) {
        return (try.abort(
          stringr::str_interp('Expected Parameter<users> as either a scalar|vector character or a list of characters but got ${class(users)[[1]]}'),
          call=caller.env, stop.on.error=stop.on.error
        ))
      }

      if (!self$connected) {
        return (try.abort('Unable to drop table: no active, valid database connection found', call=caller.env, stop.on.error=stop.on.error))
      }

      stmt = ''
      if (all((is.na(tables) | is.null(tables)) == FALSE)) {
        names = do.call(paste, append(lapply(tables, function (x) (mark.safe.value(toupper(x)))), list(sep=', ')))
        stmt = lapply(users, function (user) {
          return (stringr::str_interp("
            FOR v AS CUR CURSOR FOR
              SELECT 'GRANT SELECT ON ' || TRIM(TABSCHEMA) || '.' || TRIM(TABNAME) || ' TO ${user}' STMT
                FROM SYSCAT.TABLES
               WHERE TABSCHEMA = '${schema}'
                 AND TABNAME IN (${names})
              DO
                EXECUTE IMMEDIATE v.STMT;
              END FOR;
          "))
        })
        stmt = do.call(paste0, stmt)
        stmt = stringr::str_interp("
          BEGIN
            DECLARE CONTINUE HANDLER FOR SQLEXCEPTION BEGIN END;
            ${stmt}
          END
        ")
      } else {
        stmt = lapply(users, function (user) {
          return (stringr::str_interp("
            FOR v AS CUR CURSOR FOR
              SELECT 'GRANT SELECT ON ' || TRIM(TABSCHEMA) || '.' || TRIM(TABNAME) || ' TO ${user}' STMT
                FROM SYSCAT.TABLES
               WHERE TABSCHEMA = '${schema}'
              DO
                EXECUTE IMMEDIATE v.STMT;
              END FOR;
          "))
        })
        stmt = do.call(paste0, stmt)
        stmt = stringr::str_interp("
          BEGIN
            DECLARE CONTINUE HANDLER FOR SQLEXCEPTION BEGIN END;
            ${stmt}
          END
        ")
      }

      try.log(
        'info', stringr::str_interp('[SAILR::Connection::Grant] Sending statement @ Time<${format(Sys.time())}>:\n${stmt}\n'),
        caller.env, suppress.logs, suppress.warnings
      )

      hnd = private$hnd
      effect <- try(
        suppressWarnings(DBI::dbExecute(hnd, stmt)),
        silent=TRUE
      )

      time.called = round((microbenchmark::get_nanotime() - time.called)*1e-9, 4)
      try.log(
        'info', stringr::str_interp('[SAILR::Connection::Grant] Statement processed | Elapsed time: ${time.called}s\n'),
        caller.env, suppress.logs, suppress.warnings
      )

      if (!rlang::is_scalar_integerish(effect)) {
        msg = effect
        if (rlang::is_character(effect) & !rlang::is_scalar_character(effect)) {
          msg <- effect[[1]]
        } else if (!rlang::is_scalar_character(effect)) {
          msg <- SAILR.MSGS$UNKNOWN
        }

        return (try.abort(stringr::str_interp('Failed to grant table access:\n\n1. Statement:\n${stmt}\n\n2. Error:\n${msg}'), call=caller.env, stop.on.error=stop.on.error))
      }

      return (TRUE)
    },

    #' @title Connection$drop
    #'
    #' @description
    #' Attempts to drop a table
    #'
    #' @param table.reference (\code{character})\cr
    #'   The table schema & name reference in the shape of \code{[SCHEMA].[TABLE]}
    #' @param ignore.extinct (\code{logical})\cr
    #'   Whether to ignore non-existent tables; defaults to \code{TRUE}
    #' @param use.udf (\code{logical})\cr
    #'   Whether to use the user-defined \code{DROP_IF_EXISTS} function found in the \code{PR_SAIL} database; defaults to \code{option(SAILR.DROP.BY.UDF=TRUE)}
    #' @param stop.on.error (\code{logical})\cr
    #'   Whether to return a \code{FALSE} logical when an error is encountered instead of stopping the execution of the parent thread; defaults to \code{option(SAILR.THROW.ERRORS=TRUE)}
    #' @param suppress.logs (\code{logical})\cr
    #'   Whether to suppress message logs; defaults to \code{option(SAILR.QUIET=FALSE)}
    #' @param suppress.warnings (\code{logical})\cr
    #'   Whether to suppress warnings; defaults to \code{option(SAILR.NO.WARN=FALSE)}
    #'
    #' @return A logical that describes whether the action was successful
    #'
    drop = function (
      table.reference   = NA,
      ignore.extinct    = TRUE,
      use.udf           = getOption('SAILR.DROP.UDF', TRUE),
      stop.on.error     = getOption('SAILR.THROW.ERRORS', TRUE),
      suppress.logs     = getOption('SAILR.QUIET', FALSE),
      suppress.warnings = getOption('SAILR.NO.WARN', FALSE)
    ) {
      caller.env = rlang::caller_env(1)
      time.called = microbenchmark::get_nanotime()

      use.udf = coerce.boolean(use.udf, default=TRUE)
      ignore.extinct = coerce.boolean(ignore.extinct, default=TRUE)

      ref = get.table.reference(table.reference)
      if (!is.list(ref)) {
        reason = attr(ref, 'reason')
        if (!rlang::is_scalar_character(reason)) {
          reason = SAILR.MSGS$NAMEERR
        }

        return (try.abort(stringr::str_interp('Invalid table reference with validation error: ${reason}'), call=caller.env, stop.on.error=stop.on.error))
      }

      if (!self$connected) {
        return (try.abort('Unable to drop table: no active, valid database connection found', call=caller.env, stop.on.error=stop.on.error))
      }

      try.log(
        'info', stringr::str_interp('[SAILR::Connection::Drop] Sending statement @ Time<${format(Sys.time())}>\n'),
        caller.env, suppress.logs, suppress.warnings
      )

      hnd = private$hnd
      if (ignore.extinct & use.udf) {
        effect <- try(
          suppressWarnings(DBI::dbExecute(hnd, stringr::str_interp("CALL FNC.DROP_IF_EXISTS('${table.reference}')"))),
          silent=TRUE
        )
      } else if (ignore.extinct) {
        effect <- try(
          suppressWarnings(DBI::dbExecute(hnd, stringr::str_interp("
            BEGIN
              DECLARE CONTINUE HANDLER FOR SQLEXCEPTION BEGIN END;
              EXECUTE IMMEDIATE 'DROP TABLE ${table.reference}';
            END
          "))),
          silent=TRUE
        )
      } else {
        effect <- try(
          suppressWarnings(DBI::dbExecute(hnd, stringr::str_interp("DROP TABLE ${table.reference}"))),
          silent=TRUE
        )
      }

      time.called = round((microbenchmark::get_nanotime() - time.called)*1e-9, 4)
      try.log(
        'info', stringr::str_interp('[SAILR::Connection::Drop] Statement processed | Elapsed time: ${time.called}s\n'),
        caller.env, suppress.logs, suppress.warnings
      )

      if (!rlang::is_scalar_integerish(effect)) {
        msg = effect
        if (rlang::is_character(effect) & !rlang::is_scalar_character(effect)) {
          msg <- effect[[1]]
        } else if (!rlang::is_scalar_character(effect)) {
          msg <- SAILR.MSGS$UNKNOWN
        }

        return (try.abort(stringr::str_interp('Failed to drop table ${table.reference} with error:\n\n${msg}'), call=caller.env, stop.on.error=stop.on.error))
      }

      res = TRUE
      attr(res, 'rows.affected') = effect
      return (res)
    },

    #' @title Connection$truncate
    #'
    #' @description
    #' Attempts to truncate a table
    #'
    #' @param table.reference (\code{character})\cr
    #'   The table schema & name reference in the shape of \code{[SCHEMA].[TABLE]}
    #' @param reuse.storage (\code{logical})\cr
    #'   Whether to specify that all storage allocated to this table will continue to be allocated for the same table; defaults to \code{FALSE}
    #' @param obey.del.triggers (\code{logical})\cr
    #'   Whether to specify that we want to throw an error if delete triggers are defined for this table; defaults to \code{FALSE}
    #' @param stop.on.error (\code{logical})\cr
    #'   Whether to return a \code{FALSE} logical when an error is encountered instead of stopping the execution of the parent thread; defaults to \code{option(SAILR.THROW.ERRORS=TRUE)}
    #' @param suppress.logs (\code{logical})\cr
    #'   Whether to suppress message logs; defaults to \code{option(SAILR.QUIET=FALSE)}
    #' @param suppress.warnings (\code{logical})\cr
    #'   Whether to suppress warnings; defaults to \code{option(SAILR.NO.WARN=FALSE)}
    #'
    #' @return A logical that describes whether the action was successful
    #'
    truncate = function (
      table.reference   = NA,
      reuse.storage     = FALSE,
      obey.del.triggers = FALSE,
      stop.on.error     = getOption('SAILR.THROW.ERRORS', TRUE),
      suppress.logs     = getOption('SAILR.QUIET', FALSE),
      suppress.warnings = getOption('SAILR.NO.WARN', FALSE)
    ) {
      caller.env = rlang::caller_env(1)
      time.called = microbenchmark::get_nanotime()

      reuse.storage = coerce.boolean(reuse.storage, default=FALSE)
      obey.del.triggers = coerce.boolean(obey.del.triggers, default=FALSE)

      ref = get.table.reference(table.reference)
      if (!is.list(ref)) {
        reason = attr(ref, 'reason')
        if (!rlang::is_scalar_character(reason)) {
          reason = SAILR.MSGS$NAMEERR
        }

        return (try.abort(stringr::str_interp('Invalid table reference with validation error: ${reason}'), call=caller.env, stop.on.error=stop.on.error))
      }

      if (!self$connected) {
        return (try.abort('Unable to truncate table: no active, valid database connection found', call=caller.env, stop.on.error=stop.on.error))
      }

      try.log(
        'info', stringr::str_interp('[SAILR::Connection::Grant] Sending statement @ Time<${format(Sys.time())}>\n'),
        caller.env, suppress.logs, suppress.warnings
      )

      hnd = private$hnd
      query = 'TRUNCATE TABLE ${table.reference}'
      if (reuse.storage & obey.del.triggers) {
        query = paste(query, 'REUSE STORAGE', 'RESTRICT WHEN DELETE TRIGGERS', 'IMMEDIATE;', sep='\n\t')
      } else if (reuse.storage) {
        query = paste(query, 'REUSE STORAGE', 'IMMEDIATE;', sep='\n\t')
      } else if (obey.del.triggers) {
        query = paste(query, 'RESTRICT WHEN DELETE TRIGGERS', 'IMMEDIATE;', sep='\n\t')
      } else {
        query = paste(query, 'IMMEDIATE;', sep='\n\t')
      }

      effect <- try(
        suppressWarnings(DBI::dbExecute(hnd, stringr::str_interp(query))),
        silent=TRUE
      )

      time.called = round((microbenchmark::get_nanotime() - time.called)*1e-9, 4)
      try.log(
        'info', stringr::str_interp('[SAILR::Connection::Truncate] Statement processed | Elapsed time: ${time.called}s\n'),
        caller.env, suppress.logs, suppress.warnings
      )

      if (!rlang::is_scalar_integerish(effect)) {
        msg = effect
        if (rlang::is_character(effect) & !rlang::is_scalar_character(effect)) {
          msg <- effect[[1]]
        } else if (!rlang::is_scalar_character(effect)) {
          msg <- SAILR.MSGS$UNKNOWN
        }

        return (try.abort(stringr::str_interp('Failed to truncate table ${table.reference} with error:\n\n${msg}'), call=caller.env, stop.on.error=stop.on.error))
      }

      res = TRUE
      attr(res, 'rows.affected') = effect
      return (res)
    },

    #' @title Connection$get.hnd
    #'
    #' @description
    #' Attempts to retrieve the stored ODBC handle
    #'
    #' @return Either (a) the stored \code{DBIConnection} handle of type \code{S4}; or (b) if not connected: \code{NA}
    #'
    get.hnd = function () {
      return (private$hnd)
    },

    #' @title Connection$get.profile
    #'
    #' @description
    #' Attempts to retrieve the stored \code{SAILR::Profile}
    #'
    #' @return Either (a) the stored \code{SAILR::Profile} of type \code{R6} if initialised; or (b) \code{NA}
    #'
    get.profile = function () {
      return (private$profile)
    }
  ),

  private = list(
    #' @field connected (\code{S4|NA})\cr
    #'   A private field referencing the ODBC handle
    #'
    hnd = NA,
    #' @field connected (\code{character|NA})\cr
    #'   A private field referencing the sanitised connection string used to connect to the database
    #'
    cstring = NA,
    #' @field profile (\code{Profile|NA})\cr
    #'   A private field referencing the R6 SAILR::Profile class
    #'
    profile = NA,
    #' @field using.profile (\code{logical})\cr
    #'   A private field specifying whether this connection is using a profile
    #'
    using.profile = FALSE,

    #' @title Connection$finalize
    #'
    #' @description
    #' Destructor to cleanup on garbage collection
    #'
    finalize = function () {
      if (self$connected) {
        DBI::dbDisconnect(private$hnd)
        private$hnd = NA
      }
    },

    #' @title Connection$connect
    #'
    #' @description
    #' Creates a new connection
    #'
    #' @param username (\code{character})\cr
    #'   An optional username
    #' @param password (\code{character})\cr
    #'   An optional password
    #' @param database (\code{character})\cr
    #'   An optional database name; defaults to \code{SAILR.DEF$DATABASE} constant
    #'
    #' @return An invisible logical reflecting a successful connection
    #'
    connect = function (
      username    = NA,
      password    = NA,
      database    = SAILR.DEF$DATABASE
    ) {
      profile = private$profile
      using.profile = private$using.profile
      database = ifelse(rlang::is_scalar_character(database), database, SAILR.DEF$DATABASE)

      has.username = !is.string.empty(username)
      if (!has.username) {
        if (using.profile) {
          username = profile$system.user
        } else {
          username = prompt.client('username', default.result='')
        }
      }

      has.password = rlang::is_scalar_character(password)
      if (!has.password) {
        if (using.profile && profile$has.secrets(username, database)) {
          password = profile$get.secrets(username, database)
          if (is.na(password)) {
            password = prompt.client('password', default.result='')
          }
        } else {
          password = prompt.client('password', default.result='')
        }
      }

      autharg = list(drv=odbc::odbc(), DSN=database, UID=username, PWD=password)
      cstring = private$get.connection.string(autharg)

      hnd = tryCatch(
        suppressWarnings(do.call(DBI::dbConnect, autharg)),
        error = function (e) {
          conditionMessage(e)
        }
      )

      if ((!isS4(hnd) | !inherits(hnd, 'DBIConnection')) || !DBI::dbIsValid(hnd)) {
        if (is.string.empty(hnd)) {
          reason <- SAILR.MSGS$UNKNOWN
          spacer <- ' '
        } else {
          reason <- hnd
          spacer <- '\n\n'
        }

        if (using.profile) {
          if (profile$has.secrets(username, database) && do.call(confirm.client, SAILR.MSGS$AUTH$RM.KEYS)) {
            profile.removed = profile$remove.secrets(username=username, database=database)
          }

          if (do.call(confirm.client, SAILR.MSGS$AUTH$RETRY)) {
            return (private$connect(database=SAILR.DEF$DATABASE))
          }
        }

        return (try.abort(stringr::str_interp('Connection<`${cstring}`> failed with the following reason:${spacer}${reason}'), call=rlang::caller_env(1), stop.on.error=getOption('SAILR.THROW.ERRORS', TRUE)))
      }

      private$hnd = hnd
      private$cstring = cstring
      if (using.profile && !profile$is.secret(username, password, database)) {
        if (do.call(confirm.client, SAILR.MSGS$AUTH$SAVE)) {
          profile$set.secrets(username=username, password=password, database=database)
        }
      }

      return (invisible(TRUE))
    },

    #' @title Connection$get.connection.string
    #'
    #' @description
    #' Builds a connection string
    #'
    #' @param params (\code{list|NA})\cr
    #'   An optional parameter list
    #'
    #' @return A connection string derived from either the given parameters or the current connection HND
    #'
    get.connection.string = function (params) {
      if (rlang::is_list(params)) {
        password = params$PWD
        if (rlang::is_character(password)) {
          password = ifelse(!rlang::is_scalar_character(password), password[1], password)
          password = stringr::str_dup('*', pmax(nchar(password), 1))
        } else {
          password = '*'
        }

        cstring = append(params[which(names(params) != 'PWD')], list(PWD=password))
        cstring = paste0(names(cstring), '=', unlist(cstring, use.names=FALSE), collapse=';')
        return (cstring)
      }

      return (SAILR.MSGS$EMPTY)
    }
  ),

  active = list(
    #' @field connected (\code{logical})\cr
    #'   A read-only field describing whether this connection is currently active and whether it is valid
    #'
    connected = function () {
      hnd = private$hnd
      if (!isS4(hnd) | !inherits(hnd, 'DBIConnection')) {
        return (FALSE)
      }

      valid = try(suppressWarnings(DBI::dbIsValid(hnd)), silent=TRUE)
      return (ifelse(rlang::is_logical(valid), valid, FALSE))
    },

    #' @field connected (\code{character|NA})\cr
    #'   A read-only field describing the connection string used to connect to the database
    #'
    connection.string = function () {
      return (private$cstring)
    },

    #' @field connected (\code{character|NA})\cr
    #'   A read-only field referecing the username used to connect to the database
    #'
    username = function () {
      cstring = private$cstring
      if (!rlang::is_scalar_character(cstring)) {
        return (NA)
      }

      uid = stringr::str_match(cstring, 'UID=([^;]+)(;|$)')
      return (ifelse(all(is.na(uid)) == FALSE, uid[2], '[UID]'))
    },

    #' @field connected (\code{character|NA})\cr
    #'   A read-only field describing the name of the connected database
    #'
    database = function () {
      cstring = private$cstring
      if (!rlang::is_scalar_character(cstring)) {
        return (NA)
      }

      dsn = stringr::str_match(cstring, 'DSN=([^;]+)(;|$)')
      return (ifelse(all(is.na(dsn)) == FALSE, dsn[2], '[DSN]'))
    }
  )
)


#' @title Connection$is
#'
#' @description
#' Static method to test whether the given value is an instance of a Connection
#'
#' @param obj (\code{any})\cr
#'   Some value to test
#' @param scalar (\code{logical|NA})\cr
#'   An optional logical that determines whether the given value must be a scalar to pass the test; defaults to \code{TRUE}
#'
#' @return A logical reflecting whether the value is a Connection instance
#'
Connection$is <- function (obj) {
  if (!rlang::is_vector(obj)) {
    return (inherits(obj, 'Connection'))
  }

  scalar = coerce.boolean(scalar, default=TRUE)
  if (!scalar) {
    return (all(sapply(obj, function (x) (inherits(obj, 'Connection')))))
  }

  return (FALSE)
}


#' @title Connection$is.valid
#'
#' @description
#' Static method to test whether the given value is both an instance of a
#' Connection object, and holds a valid, active handle connection to the
#' database
#'
#' @param obj (\code{any})\cr
#'   Some value to test
#'
#' @return A logical describing whether the given object is a valid, active Connection
#'
Connection$is.valid <- function (obj) {
  if (!Connection$is(obj)) {
    return (FALSE)
  }

  return (obj$connected)
}
