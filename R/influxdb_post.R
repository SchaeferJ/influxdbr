#' @title send POST to an InfluxDB server
#' @description This function sends POST to an InfluxDB server. It is not 
#' exported and only used for some helper functions within this package.
#' @inheritParams influx_query
#' @return A tibble or NULL
#' @references \url{https://influxdb.com/}
#' @keywords internal
influx_post <- function(con,
                        db = NULL,
                        query = "") {
  
  q <- list(db = db,
            u = con$user,
            p = con$pass,
            q = query)
    
  response <- httr_POST(con = con, query = q, endpoint = "query", csv = T)

  check_response_errors(response, FALSE)
  
  out <- list(content = httr::content(response, "text", encoding = "UTF-8"),
              query = query,
              status = httr::http_status(response))
  class(out) <- "invluxdbr.post.response"
  out
}
