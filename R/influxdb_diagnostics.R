#' @title Show diagnostics and stats.
#' @description These functions calls `influx_query` to receive some stats.
#' @inheritParams influx_query
#' @seealso \code{\link[influxdbr]{influx_connection}}
#' @section Warning: \code{show_stats} might take some time.
#' @return A data.frame frame.
#' @rdname diagnostics
#' @export
show_stats <- function(con) {
  influx_query(con = con, query = "SHOW STATS") %>%
    flatten_series("STAT")
}

#' @rdname diagnostics
#' @export
show_diagnostics <- function(con) {
  influx_query(con = con, query = "SHOW DIAGNOSTICS") %>%
    flatten_series("DIAGNOSTIC")
}

flatten_series <- function(series, name) {
  lapply(series,
         function(s) {
           data.frame(category = s[["name"]],
                      measure = names(s)[-1],
                      value = unlist(s[-1], use.names = F))
         }) %>% 
    data.table::rbindlist() %>%
    data.table::setnames(c("CATEGORY", name, "VALUE")) %>%
    as.data.frame()
}
