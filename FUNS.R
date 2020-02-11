# Reads certain columns of a Screaming Frog crawl -----

read_crawl <- function(file_path) {
  
  crawl <- read_csv(file_path,
                    quoted_na = FALSE,
                    col_types = cols_only(
                      `Address` = col_character(),
                      `Content` = col_character(),
                      `Meta Robots 1` = col_character(),
                      `Status Code` = col_number(),
                      `Status` = col_character(),
                      `Indexability` = col_character(), # TODO: KEEP?
                      `Indexability Status` = col_character(), # TODO: KEEP?
                      `Title 1` = col_character(),
                      `Meta Description 1` = col_character(),
                      `H1-1` = col_character(),
                      `Canonical Link Element 1` = col_character(),
                      `Crawl Depth` = col_number(),
                      `Size (bytes)` = col_number(),
                      `Word Count` = col_number())) %>%
    janitor::clean_names() %>%
    mutate_at(c("meta_robots_1", "content"),
              tolower) %>%
    mutate_at(c("meta_robots_1", "content"),
              str_replace_all, pattern = "\\s", replacement = "") %>%
    mutate(indexability_status = if_else(indexability_status == "", "Indexable", indexability_status))
  
}


# Adds multiple URLs to a thread-pool -----------------
# and fetches their HTTP status code ------------------

fetch_multi_urls <- function(urls,
                             total_con = 50, # max. simultaneous conntections 
                             host_con = 5, # max simultaneous connections per host
                             multiplex = TRUE, # use HTTP2 multiplex
                             failonerror = FALSE, # fail if URL respondes with 404, 500 etc.
                             nobody = TRUE, # GET / HEAD
                             followlocation = FALSE, # follow redirect
                             ssl_verifypeer = 0 # check SSL certificate; 0 = no, 1 = yes
) {
  
  results <- list()
  refetch <- c()
  
  # callback function
  cb <- function(res) {
    
    # check if URL responded with 500 Server error
    if (res$status_code >= 500) {
      
      # checks if the 500-URL is already in the `refretch`
      # if true: the URL will not be requested again, but the statemant
      # returns the Status Code 500
      if (res$url %in% refetch) {
        
        results <<- append(results, list(res))
        message("\tURL: ",
                res$url,
                "\n\tStatus: ",
                crayon::bgYellow$bold(res$status_code), "\n")
        
        # if false: the URL is added to the pool again and and a flag is 
        # set in `refetch`
      } else {
        
        curl::curl_fetch_multi(res$url,
                               done = cb,
                               handle = curl::new_handle(
                                 failonerror = failonerror,
                                 nobody = nobody,
                                 followlocation = followlocation,
                                 ssl_verifypeer = ssl_verifypeer),
                               pool = p)
        
        refetch <<- append(refetch, res$url)
        
        message("\tURL: ",
                res$url,
                "\n\tStatus: ",
                crayon::blue$bold(res$status_code),
                "\n~ URL added to pool again\n")
        
      }
      
      # if URL does not responded with 500, write Status Code to `results`
    } else {
      
      results <<- append(results, list(res))
      
      if (res$status_code < 300) {
        
        message("\tURL: ",
                res$url,
                "\n\tStatus: ",
                crayon::green$bold(res$status_code),
                "\n")
        
      } else if (res$status_code < 400){
        
        message("\tURL: ",
                res$url,
                "\n\tStatus: ",
                crayon::yellow$bold(res$status_code),
                "\n")
        
      } else if (res$status_code < 500) {
        
        message("\tURL: ",
                res$url,
                "\n\tStatus: ",
                crayon::red$bold(res$status_code),
                "\n")
        
      }
    }
  }
  
  # config URL pool
  p <- curl::new_pool(total_con = total_con,
                      host_con = host_con,
                      multiplex = multiplex)
  
  # add URLs to pool
  for (url in urls) {
    
    curl::curl_fetch_multi(url,
                           done = cb,
                           handle = curl::new_handle(failonerror = failonerror,
                                                     nobody = nobody,
                                                     followlocation = followlocation,
                                                     ssl_verifypeer = ssl_verifypeer),
                           pool = p)
    
  }
  
  # initialize requests
  r <- curl::multi_run(pool = p)
  
  return(results)
  
}



# Format metric names to display names ----------------

format_metric_name <- function(metric) {
  
  str_to_title(
    str_replace_all(
      str_replace(metric,"change_", ""),"_", " "))
  
}



# Add sheets in a vector to workbook ------------------

add_sheets <- function(workbook, metric) {
  
  assign(paste0("sheet_", metric),
         createSheet(workbook, format_metric_name(metric)))
  
  addDataFrame(eval(parse(text = metric)),
               sheet = eval(parse(text = paste0("sheet_", metric))),
               row.names = FALSE)
  
}
