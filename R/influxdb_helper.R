#' method send the httr::GET command
#' function is not exported
#' @param con influx_connection object
#' @param endpoint api endpoint
#' @return httr::GET response
#' @keywords internal
httr_GET <- function(con, query = NULL, endpoint, csv = TRUE) {
  config <- con$config
  if (csv) {
    config$headers[["Accept"]] <- "application/csv"
  }
  httr::GET(url = "",
            scheme = con$scheme,
            hostname = con$host,
            port = con$port,
            path = paste0(con$path, endpoint),
            query = query,
            config = config)
}


#' method send the httr::POST command
#' function is not exported
#' @param con influx_connection object
#' @param endpoint api endpoint
#' @return httr::POST response
#' @keywords internal
httr_POST <- function(con, query = NULL, body = NULL, endpoint, csv = TRUE) {
  config <- con$config
  if (csv) {
    config$headers[["Accept"]] <- "application/csv"
  }
  httr::POST(url = "",
             body = body,
             scheme = con$scheme,
             hostname = con$host,
             port = con$port,
             path = paste0(con$path, endpoint),
             query = query,
             config = config)
}

#' method to check the server communication results
#' function is not exported
#' @param x httr::POST response
#' @keywords  internal
check_response_errors <- function(response, throw_on_empty = FALSE) {
  
  # query:
  # HTTP status code	Description
  # 200 OK	Success! The returned JSON offers further information.
  # 400 Bad Request	Unacceptable request. Can occur with a syntactically incorrect 
  #     query. The returned JSON offers further information.
  # 401 Unauthorized	Unacceptable request. Can occur with invalid authentication 
  #     credentials.
  
  # write:
  # HTTP status code	Description
  # 204 No Content	Success!
  # 400 Bad Request	Unacceptable request. Can occur with a Line Protocol syntax 
  #     error or if a user attempts to write values to a field that previously 
  #     accepted a different value type. The returned JSON offers further information.
  # 401 Unauthorized	Unacceptable request. Can occur with invalid authentication 
  #     credentials.
  # 404 Not Found	Unacceptable request. Can occur if a user attempts to write to
  #     a database that does not exist. The returned JSON offers further information.
  # 500 Internal Server Error	The system is overloaded or significantly impaired. 
  #     Can occur if a user attempts to write to a retention policy that does not exist. 
  #     The returned JSON offers further information.
  
  if (!httr::status_code(response) %in% c(200, 204)) {
    if (length(response$content) == 0) {
      rethrow_errors_with_json(response)
    }
    stop(httr::content(response, "text", encoding = "UTF-8"), call. = FALSE)
  } else if (length(response$content) == 0 && throw_on_empty) {
    rethrow_errors_with_json(response)
  }
  
  return(NULL)
}

rethrow_errors_with_json <- function(resp) {
  ## Old versions of influx (1.1.1) return zero length error body when
  ## application/csv header. As a work around we re-send the request without
  ## application/csv header.
  url <- resp$url
  method <- resp$request$method
  headers <- resp$request$headers
  headers[["Accept"]] <- "application/json"
  resp2 <-
    if (method == "POST") {
      httr::POST(url = url, headers = headers)
    } else {
      httr::GET(url = url, headers = headers)
    }
  if (length(resp2$content) == 0)
    ## should not get here, but if we did ...
    stop("influxdb returned an empty error message. A very old influxdb version?")
  else
    stop(httr::content(resp2, "text", encoding = "UTF-8"), call. = FALSE)
}

#' method to transform precision divisor
#' function is not exported
#' @param x character
#' @keywords  internal
precision_divisor <- function(x) {
  switch(
    x,
    "ns" = 1e+9,
    "n" = 1e+9,
    "u" = 1e+6,
    "ms" = 1e+3,
    "s" = 1,
    "m" = 1 / 60,
    "h" = 1 / (60 * 60),
    stop(sprintf("bad precision argument (%s)", x)))
}

#' Paste non-null key-value pairs for queries
#' @param ... key value pair
#' @keywords internal
qpaste <- function(...) {
  dots <- list(...)
  dots <- dots[!sapply(dots, is.null)]
  dots <- paste(toupper(names(dots)), dots)
  gsub("^ *", "", do.call(paste, list(dots, collapse = " ")))
}
