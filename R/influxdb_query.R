#' @title Query an InfluxDB server
#' @description This functions queries an InfluxDB server.
#' @param con An `influx_connection` object
#'   (s. \code{\link{influx_connection}}).
#' @param db Sets the target database for the query.
#' @param query The InfluxDB query to be sent.
#' @param timestamp_format Sets the timestamp format ("n", "u", "ms", "s", "m",
#'   "h").
#' @param verbose If TRUE print log messages.
#' @return A list a data.frame object. Empty query returns NULL.
#' @rdname influx_query
#' @seealso \code{\link[influxdbr]{influx_connection}}
#' @references \url{https://docs.influxdata.com/influxdb/}
#' @export
influx_query <- function(con,
                         db = NULL,
                         query = "SELECT * FROM measurement",
                         timestamp_format = c("n", "u", "ms", "s", "m", "h"),
                         split_tags = TRUE,
                         tags_as_factors = TRUE,
                         as_POSIXct = TRUE, 
                         verbose = FALSE) {

  ## cannot match responses to queries if some are empty
  ## https://github.com/influxdata/influxdb/issues/14274
  if (grepl(";", query)) {
    return (lapply(strsplit(query, ";", fixed = TRUE)[[1]],
                   function(q) {
                     .influx_query(con = con, db = db, query = q,
                                   timestamp_format = timestamp_format,
                                   split_tags = split_tags,
                                   tags_as_factors = tags_as_factors,
                                   as_POSIXct = as_POSIXct,
                                   verbose = verbose)
                   }))
  }

  .influx_query(con = con, db = db, query = query,
                timestamp_format = timestamp_format,
                split_tags = split_tags,
                tags_as_factors = tags_as_factors,
                as_POSIXct = as_POSIXct,
                verbose = verbose)  
}

.influx_query <- function(con,
                          db = NULL,
                          query = "SELECT * FROM measurement",
                          timestamp_format = c("n", "u", "ms", "s", "m", "h"),
                          split_tags = TRUE,
                          tags_as_factors = TRUE,
                          as_POSIXct = TRUE, 
                          verbose = FALSE,
                          throw_on_empty = FALSE) {

  stopifnot(length(query) == 1)

  epoch <- match.arg(timestamp_format)
  
  q <- list(db = db,
            u = con$user,
            p = con$pass,
            q = query,
                                        # chunked influx request is 25% faster
            chunked = "true", 
            epoch = epoch)

  if (verbose) log("Sending influx query ...")
  response <- httr_GET(con = con, query = q, endpoint = "query", csv = TRUE)

  check_response_errors(response, throw_on_empty)

  if (verbose) log("Parsing csv ...")

  content <- httr::content(response, "text", encoding = "UTF-8")
  
  if (nzchar(content)) {
    out <-
      strsplit(content, "\n\n", fixed = T)[[1]] %>% 
      lapply(parse_csv,
             split_tags = split_tags,
             tags_as_factors = tags_as_factors,
             epoch = epoch,
             as_POSIXct = as_POSIXct)
    if (verbose) log("Done!")
    if (length(out) > 1) out
    else out[[1]]
  }
}

# INFLUX BUG: https://github.com/influxdata/influxdb/issues/14275
# Quotes within strings are duplicated:
replace_double_quote <- function(x) {
  gsub("\"\"", "\"", x, fixed = TRUE)
}

split_to_named <- function(x) {
  # get rid of \\ preceding , =
  x <- gsub("\\\\([, =])", "\\1", x)
  x <- replace_double_quote(x)
  seq <- seq.int(1, length(x), by = 2)
  structure(as.list(x[seq + 1]), names = x[seq])
}

parse_csv <- function(x, split_tags = TRUE, tags_as_factors = TRUE,
                      epoch = "n", as_POSIXct = TRUE) {
  ## cat(x)
  out <- data.table::fread(text = x)
  if (split_tags & !is.null(out[["tags"]])) {
    tags <- as.factor(out[["tags"]])
    ix <- as.integer(tags)
    out[["tags"]] <- NULL
    kvs <-
      # split by ,= which are not preceded by \\
      strsplit(levels(tags), "(?<!\\\\)[,=]", perl = TRUE) %>%
      lapply(split_to_named) %>%
      data.table::rbindlist(fill = TRUE, use.names = TRUE)
    tag_factors <- lapply(kvs, function(levels) levels[ix])
    processor <- if (tags_as_factors) as.factor else identity
    for (nm in names(kvs)) {
      out[[nm]] <- processor(kvs[[nm]][ix])
    }
  }
  if (as_POSIXct && !is.null(out[["time"]])) {
    out[["time"]] <- .POSIXct(out[["time"]]/precision_divisor(epoch), tz = "UTC")
  }
  for (nm in names(out)) {
    if (is.character(out[[nm]]))
      out[[nm]] <- replace_double_quote(out[[nm]])
  }
  names(out) <- replace_double_quote(names(out))
  class(out) <- c("influxdbr.response", "data.frame")
  out
}
  
log <- function(...) {
  str <- paste(..., collapse = "", sep = "")
  message(sprintf("[%s] %s", Sys.time(), str))
}

