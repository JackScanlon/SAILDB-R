#' @title Profile
#'
#' @rdname SAIL-Profile
#'
#' @description
#' User profile secret manager for SAIL DB authentication
#'
#' @details
#' Profile secret manager for SAIL DB authentication; exported to allow for use in your
#' personal projects and/or to better manage your own secrets
#'
#' @import R6
#' @import keyring
#' @import stringr
#' @import vctrs
#' @import rstudioapi
#'
#' @export
#'
Profile <- R6::R6Class(
  'Profile',
  public = list(
    #' @title Profile$new
    #'
    #' @description
    #' Initialise a user profile instance
    #'
    #' @param keychain.name (\code{character|NA})\cr
    #'   A keychain name; defaults to \code{SAILR.DEF$KEYCHAIN} if no name is given
    #'
    #' @return A new user profile instance
    #'
    initialize = function (keychain.name = NA) {
      private$keychain = ifelse(rlang::is_scalar_character(keychain.name), keychain.name, SAILR.DEF$KEYCHAIN)
    },

    #' @title Profile$has.secrets
    #'
    #' @description
    #' Method to check whether a secret exists for the given username and database
    #'
    #' @param username (\code{character|NA})\cr
    #'   Optional username that relates to these secrets
    #' @param database (\code{character|NA})\cr
    #'   The name of the database that these secrets relate to; defaults to \code{SAILR.DEF$DATABASE} if no name is given
    #'
    #' @return A logical describing whether any secrets exist
    #'
    has.secrets = function (username = NA, database = SAILR.DEF$DATABASE) {
      if (!rlang::is_scalar_character(database)) {
        return (NA)
      }

      service.name = paste0(private$keychain, '/', database)
      keyring.entries = keyring::key_list(service=service.name)

      has.keys = ifelse(is.data.frame(keyring.entries), nrow(keyring.entries) > 0, FALSE)
      if (has.keys && !rlang::is_scalar_character(username)) {
        return (TRUE)
      } else if (has.keys) {
        return (username %in% keyring.entries[, 'username'])
      }

      return (FALSE)
    },

    #' @title Profile$get.secrets
    #'
    #' @description
    #' Method to collect the secrets associated with the given username and database
    #'
    #' @param username (\code{character|NA})\cr
    #'   A username that relates to these secrets
    #' @param database (\code{character|NA})\cr
    #'   The name of the database that these secrets relate to; defaults to \code{SAILR.DEF$DATABASE} if no name is given
    #'
    #' @return The secrets associated with this username & database if the key exists
    #'
    get.secrets = function (username = NA, database = SAILR.DEF$DATABASE) {
      params.valid = all(lapply(list(username, database), rlang::is_scalar_character) == TRUE)
      if (!params.valid) {
        return (NA)
      }

      service.name = paste0(private$keychain, '/', database)
      keyring.entries = keyring::key_list(service=service.name)

      has.keys = ifelse(is.data.frame(keyring.entries), nrow(keyring.entries) > 0, FALSE)
      if (!has.keys) {
        return (NA)
      } else if (!username %in% keyring.entries[, 'username']) {
        return (NA)
      }

      return (keyring::key_get(service.name, username))
    },

    #' @title Profile$is.secret
    #' @description
    #' Compare a known secret to the value stored in the keychain
    #'
    #' @param username (\code{character|NA})\cr
    #'   Optional username that relates to these secrets
    #' @param password (\code{character|NA})\cr
    #'  The secret you would like to compare to any stored in the keychain
    #' @param database (\code{character|NA})\cr
    #'   The name of the database that these secrets relate to; defaults to \code{SAILR.DEF$DATABASE} if no name is given
    #'
    #' @return A logical describing the equivalence between the secrets
    #'
    is.secret = function (username = NA, password = NA, database = SAILR.DEF$DATABASE) {
      params.valid = all(lapply(list(username, password, database), rlang::is_scalar_character) == TRUE)
      if (!params.valid) {
        return (FALSE)
      }

      secret = self$get.secrets(username, database)
      if (is.na(secret)) {
        return (FALSE)
      }

      return (secret == password)
    },

    #' @title set.secrets
    #'
    #' @description
    #' Method to set the secrets for the given username and database
    #'
    #' @param username (\code{character|NA})\cr
    #'   A username that relates to these secrets
    #' @param password (\code{character|NA})\cr
    #'  The secret to store in the keychain
    #' @param database (\code{character|NA})\cr
    #'   The name of the database that these secrets relate to; defaults to \code{SAILR.DEF$DATABASE} if no name is given
    #'
    #' @return A logical describing whether this action was successful
    #'
    set.secrets = function (username = NA, password = NA, database = SAILR.DEF$DATABASE) {
      params.valid = all(lapply(list(username, password, database), rlang::is_scalar_character) == TRUE)
      if (!params.valid) {
        return (FALSE)
      }

      service.name = paste0(private$keychain, '/', database)
      keyring::key_set_with_value(service=service.name, username=username, password=password)
      return (TRUE)
    },

    #' @title Profile$remove.secrets
    #'
    #' @description
    #' Method to remove secrets associated with either (a) a username and a database; or (b) all secrets associated with a database
    #'
    #' @param username (\code{character|NA})\cr
    #'   An optional username that relates to these secrets; if none are provided all of the secrets associated with the database will be removed
    #' @param database (\code{character|NA})\cr
    #'   The name of the database that these secrets relate to; defaults to \code{SAILR.DEF$DATABASE} if no name is given
    #'
    #' @return A logical describing whether this was successful
    #'
    remove.secrets = function (username = NA, database = SAILR.DEF$DATABASE) {
      has.username = rlang::is_scalar_character(username)
      has.database = rlang::is_scalar_character(database)

      if (has.username && has.database) {
        if (!self$has.secrets(username, database)) {
          return (FALSE)
        }

        keyring::key_delete(paste0(private$keychain, '/', database), username)
        return (TRUE)
      } else if (has.database) {
        service.name = paste0(private$keychain, '/', database)
        keyring.entries = keyring::key_list(service=service.name)

        if (!is.data.frame(keyring.entries)) {
          return (FALSE)
        }

        for (i in nrow(keyring.entries)) {
          username = keyring.entries[i, 'username']
          if (!rlang::is_scalar_character(username)) {
            next
          }

          keyring::key_delete(service.name, username)
        }

        return (TRUE)
      }

      return (FALSE)
    }
  ),

  private = list(
    #' @field keychain (\code{character|NA})\cr
    #'   A private reference to the keychain name
    #'
    keychain = NA
  ),

  active = list(
    #' @field connected (\code{character|NA})\cr
    #'   A read-only field describing the name of the keychain
    #'
    keychain.name = function () {
      return (private$keychain)
    },

    #' @field system.user (\code{character|NA})\cr
    #'   A read-only helper field to derive the client's system username
    #'
    system.user = function () {
      return (Sys.getenv('USERNAME'))
    }
  )
)


#' @title Profile$is
#'
#' @description
#' Static method to test whether the given value is an instance of a Profile
#'
#' @param obj (\code{any})\cr
#'   Some value to test
#' @param scalar (\code{logical|NA})\cr
#'   An optional logical that determines whether the given value must be a scalar to pass the test; defaults to \code{TRUE}
#'
#' @return A logical reflecting whether the value is a Profile instance
#'
Profile$is <- function (obj) {
  if (!rlang::is_vector(obj)) {
    return (inherits(obj, 'Profile'))
  }

  scalar = coerce.boolean(scalar, default=TRUE)
  if (!scalar) {
    return (all(sapply(obj, function (x) (inherits(obj, 'Profile')))))
  }

  return (FALSE)
}


#' @title Profile$clear
#'
#' @description
#' Static method to clear secrets associated with a username and/or datasource
#'
#' @details
#' The \code{select} argument of this method expects the character string to be composed as one of the following:
#' \enumerate{
#'   \item \code{c('USERNAME')} -- clears all secrets for a given username for the default \code{SAILR.DEF$DATABASE}
#'   \item \code{c('DATASOURCE_NAME:USERNAME')} -- clears the secrets for username within the given datasource, \emph{e.g.} \code{PR_SAIL:USERNAME}
#' }
#'
#' @usage SAILR::Profile$clear(c('PR_SAIL', 'OTHER_DSN:USERNAME'))
#'
#' @param select (\code{vector|list})\cr
#'   A vector or flat list of username and/or datasource-username character strings
#' @param keychain (\code{character|NA})\cr
#'   An optional keychain name; defaults to \code{SAILR.DEF$KEYCHAIN}
#'
#' @return A logical reflecting whether the operation was successful
#'
Profile$clear <- function (select, keychain = NA) {
  caller.env = rlang::caller_env(1)

  if (!is.na(keychain) && !is.null(keychain) && !rlang::is_scalar_character(keychain)) {
    keychain = class(keychain)[1]
    try.log(
      'warn',
      stringr::str_interp('Failed to clear secrets, expected keychain parameter as character|NA but got ${keychain}'),
      caller.env
    )

    return (FALSE)
  }

  if (rlang::is_list(select)) {
    select = as.vector(unlist(select, use.names=FALSE))
  }

  if (!rlang::is_character(select)) {
    select = class(select)[1]
    try.log(
      'warn',
      stringr::str_interp('Failed to clear secrets, expected select parameter as list|vector|NA of characters but got ${select}'),
      caller.env
    )

    return (FALSE)
  }

  keychain = ifelse(rlang::is_scalar_character(keychain), keychain, SAILR.DEF$KEYCHAIN)
  for (i in 1:length(select)) {
    element = select[i]
    components = stringr::str_split(element, ':', n=2)
    if (!is.null(components) && length(components) == 2) {
      dsn = components[1]
      uid = components[2]

      service.name = paste0(keychain, '/', dsn)
      keyring::key_delete(service.name, uid)
      next
    }

    service.name <- paste0(keychain, '/', SAILR.DEF$DATABASE)
    keyring.entries = keyring::key_list(service=service.name)
    if (!is.data.frame(keyring.entries) || nrow(keyring.entries) < 1) {
      next
    }

    for (j in 1:nrow(keyring.entries)) {
      username = keyring.entries[j, 'username']
      if (username != element) {
        next
      }

      keyring::key_delete(service.name, username)
    }
  }

  return (TRUE)
}
