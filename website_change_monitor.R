#                __  __                  __  _           
#    ____ ____  / /_/ /__________ ______/ /_(_)___  ____ 
#   / __ `/ _ \/ __/ __/ ___/ __ `/ ___/ __/ / __ \/ __ \
#  / /_/ /  __/ /_/ /_/ /  / /_/ / /__/ /_/ / /_/ / / / /
#  \__, /\___/\__/\__/_/   \__,_/\___/\__/_/\____/_/ /_/ 
# /____/                                                 
#
# web: https://www.gettraction.de/
# twitter: @gettraction_om
#
# author: Patrick Lürwer/David Meyer
# date: 2020-02-07  


# Libraries -------------------------------------------

options(java.parameters = "-Xmx8g")
library(tidyverse)
library(xlsx)
library(slackr)
library(lubridate)
source("FUNS.R")



# VARS ------------------------------------------------

PATH_TO_XLSX_EXPORTS <- "C:/01_crawl_abgleiche"
PATH_TO_SF_EXPORTS <- "C:/02_sf_exports"

#TESTING_WEBSITE <- ""

#SLACK_API_KEY <- ""

# Slack -----------------------------------------------

#slackr_setup(api_token = SLACK_API_KEY,
#             channel = "#scripts",
#             username = "[R] Crawl-Abgleich",
#             echo = FALSE)




# Get-files -------------------------------------------

## Find all CSVs
files <- list.files(PATH_TO_SF_EXPORTS,
                    full.names = TRUE,
                    recursive = TRUE,
                    pattern = "\\.csv")



## Split paths
df_file_paths <- tibble(path = files) %>% 
  splitstackshape::cSplit(splitCols = "path",
                          drop = FALSE,
                          sep = "/",
                          direction = "wide",
                          type.convert = FALSE) %>% 
  as_tibble()



## Extract website, datetime
select_cols <- c(1, ncol(df_file_paths) - 2, ncol(df_file_paths) - 1)

df_file_paths <- df_file_paths[ , select_cols]

names(df_file_paths) <- c("path", "website", "datetime")



## Convert to date
df_file_paths <- df_file_paths %>% 
  mutate(datetime = ymd_hms(datetime),
         date = date(datetime))



## Check if two crawls exist
df_file_paths <- df_file_paths %>% 
  group_by(website) %>% 
  filter(n() > 1) %>% 
  ungroup()



## Get last two crawls
df_file_paths <- df_file_paths %>%
  group_by(website) %>% 
  mutate(date_rank = rank(desc(date))) %>%
  filter(date_rank %in% c(1, 2)) %>% 
  ungroup()



## Get websites for iteration
websites <- unique(df_file_paths$website)



## Filter for testing
if (exists("TESTING_WEBSITE")) websites <- TESTING_WEBSITE



# Compare crawls --------------------------------------

for (WEBSITE in websites) {
  
  try_res <- tryCatch({
    
    df_website_file_paths <- df_file_paths %>% 
      filter(website == WEBSITE)
    
    CRAWL_NEW_DATE <- max(df_website_file_paths$date)
    CRAWL_OLD_DATE <- min(df_website_file_paths$date)
    
    
    message("\n\n[", WEBSITE, "] ", strrep("*", 40), "\n\n~ Read crawls\n")
    
    
    crawl_new <- read_crawl(df_website_file_paths[df_website_file_paths$date == CRAWL_NEW_DATE, "path"][[1,1]]) %>% 
      mutate(date = CRAWL_NEW_DATE)
    crawl_old <- read_crawl(df_website_file_paths[df_website_file_paths$date == CRAWL_OLD_DATE, "path"][[1,1]]) %>% 
      mutate(date = CRAWL_OLD_DATE)
    
    
    ## Create website-dir if not exists
    dir.create(file.path(PATH_TO_XLSX_EXPORTS, WEBSITE),
               showWarnings = FALSE)
    
    
    
    ## Get new (linked) URLs ---------------------------
    message("~ Get new (linked) URLs\n")
    
    new_urls <- crawl_new %>%
      filter(str_detect(content, "html") | status == "Connection Timeout") %>%
      anti_join(crawl_old %>%
                  filter(str_detect(content, "html") | status == "Connection Timeout"), 
                by = "address") %>%
      mutate(is_canonical = (address == canonical_link_element_1 | canonical_link_element_1 == "")) %>%
      select(date,
             address,
             status_code,
             status,
             indexability_status,
             meta_robots_1,
             canonical_link_element_1,
             title_1,
             meta_description_1,
             is_canonical) %>%
      mutate(date = as.character(date)) %>%
      as.data.frame() # convert to DataFrame as xlsx:: cannot handle tibble()
    
    
    
    ## Get not longer / deleted URLs -------------------
    message("~ Get not longer / deleted URLs\n")
    
    no_longer_linked_urls <- crawl_old %>%
      filter(str_detect(content, "html") | status == "Connection Timeout") %>%
      anti_join(crawl_new %>%
                  filter(str_detect(content, "html") | status == "Connection Timeout"),
                by = "address")
    
    
    ## get Status Code of not linked / deleted URLs
    message("~ Get Status Code of not linked / deleted URLs\n")
    
    urls <- no_longer_linked_urls$address
    
    if (length(urls) > 0) {
      
      r <- fetch_multi_urls(urls)
      
      r_df <- tibble(
        address = unlist(lapply(r, `[[`, "url")),
        status_code_current = unlist(lapply(r, `[[`, "status_code"))
      )
      
      no_longer_linked_urls <- no_longer_linked_urls %>%
        left_join(r_df, by = "address") %>%
        select(date,
               address,
               status_code,
               status,
               indexability_status,
               status_code_current,
               meta_robots_1,
               canonical_link_element_1,
               title_1,
               meta_description_1) %>%
        rename(status_code_old = status_code) %>%
        mutate(date = as.character(date)) %>%
        as.data.frame()
      
    }
    
    
    ## Get identical URLs ------------------------------
    message("~ Get identical URLs\n")
    
    identical_urls <- crawl_new %>%
      filter(str_detect(content, "html")) %>%
      inner_join(crawl_old %>%
                   filter(str_detect(content, "html")),
                 by = "address",
                 suffix = c("_new", "_old")) %>%
      mutate(change_status_code = status_code_new != status_code_old,
             change_canonical = canonical_link_element_1_new != canonical_link_element_1_old,
             change_index = meta_robots_1_new != meta_robots_1_old,
             change_title = title_1_new != title_1_old,
             change_description = meta_description_1_new != meta_description_1_old,
             change_content_type = content_new != content_old,
             change_crawl_depth = crawl_depth_new != crawl_depth_old,
             change_h1 = h1_1_new != h1_1_old,
             change_word_count = word_count_new != word_count_old,
             change_indexability = indexability_new != indexability_old,
             change_indexability_status = indexability_status_old != indexability_status_new,
             change_orphan_pages = (is.na(crawl_depth_old) & !is.na(crawl_depth_new)) | (!is.na(crawl_depth_old) & is.na(crawl_depth_new))) 
    
    
    
    ## Get identical URLs with changes -----------------
    message("~ Get identical URLs with changes\n")
    
    
    ## orphane pages
    message("\tOrphan pages")
    
    change_orphan_pages <- identical_urls %>%
      filter(change_orphan_pages == TRUE) %>% 
      mutate(change = case_when(!is.na(crawl_depth_old) & is.na(crawl_depth_new) ~ "neu verwaist",
                                is.na(crawl_depth_old) & !is.na(crawl_depth_new) ~ "nicht mehr verwaist")) %>% 
      select(change,
             address,
             status_code_old,
             status_code_new,
             status_old,
             status_new,
             title_1_old,
             title_1_new) %>% 
      as.data.frame()
    
    
    ## status code
    message("\tStatus code")
    
    change_status_code <- identical_urls %>%
      filter(change_status_code == TRUE) %>%
      select(address,
             status_code_new,
             status_code_old,
             status_new,
             status_old,
             indexability_status_old,
             indexability_status_new) %>%
      as.data.frame()
    
    
    ## canonical
    message("\tCanonical")
    
    change_canonical <- identical_urls %>%
      filter(change_canonical == TRUE) %>%
      .[!.$address %in% change_status_code$address, ] %>% # URLs herausfiltern, die eine Änderung beim Status Code aufweisen
      select(address,
             canonical_link_element_1_new,
             canonical_link_element_1_old) %>%
      mutate(new_is_canonical = (address == canonical_link_element_1_new | canonical_link_element_1_new ==""),
             old_was_canonical = (address == canonical_link_element_1_old | canonical_link_element_1_old == "")) %>%
      as.data.frame()

    ## meta robots
    message("\tMeta robots")
    
    change_index <- identical_urls %>%
      filter(change_index == TRUE) %>%
      .[!.$address %in% change_status_code$address, ] %>%
      select(address,
             meta_robots_1_new,
             meta_robots_1_old) %>%
      as.data.frame()
    
    ## title
    message("\tTitle")
    
    change_title <- identical_urls %>%
      filter(change_title == TRUE) %>%
      .[!.$address %in% change_status_code$address, ] %>%
      select(address,
             title_1_new,
             title_1_old) %>%
      as.data.frame()
    
    
    ## description
    message("\tDescription")
    
    change_description <- identical_urls %>%
      filter(change_description == TRUE) %>%
      .[!.$address %in% change_status_code$address, ] %>%
      select(address,
             meta_description_1_new,
             meta_description_1_old) %>%
      as.data.frame()
    
    
    ## content type
    message("\tContent type")
    
    change_content_type <- identical_urls %>%
      filter(change_content_type == TRUE) %>%
      .[!.$address %in% change_status_code$address, ] %>%
      select(address,
             content_new,
             content_old) %>%
      as.data.frame()
    
    
    ## crawl depth
    message("\tCrawl depth")
    
    change_crawl_depth <- identical_urls %>%
      filter(change_crawl_depth == TRUE | is.na(change_crawl_depth)) %>%
      mutate_at(c("crawl_depth_new", "crawl_depth_old"), as.numeric) %>%
      .[!.$address %in% change_status_code$address, ] %>%
      select(address,
             crawl_depth_new,
             crawl_depth_old) %>%
      mutate(crawl_depth_change_absolut = crawl_depth_new - crawl_depth_old) %>%
      arrange(desc(crawl_depth_change_absolut)) %>%
      as.data.frame()
    
    
    ## h1
    message("\tH1")
    
    change_h1 <- identical_urls %>%
      filter(change_h1 == TRUE) %>%
      .[!.$address %in% change_status_code$address, ] %>%
      select(address,
             h1_1_new,
             h1_1_old) %>%
      as.data.frame()
    
    
    ## word count
    message("\tWord count")
    
    change_word_count <- identical_urls %>%
      filter(change_word_count == TRUE) %>%
      .[!.$address %in% change_status_code$address, ] %>%
      select(address,
             word_count_new,
             word_count_old) %>%
      mutate(word_count_change_absolut = word_count_new - word_count_old,
             word_count_change_perc = (word_count_new / word_count_old - 1) * 100) %>%
      arrange(desc(word_count_change_absolut)) %>%
      as.data.frame()
    
    
    ## indexability
    message("\tIndexability")
    
    change_indexability <- identical_urls %>%
      filter(change_indexability == TRUE) %>%
      .[!.$address %in% change_status_code$address, ] %>%
      select(address,
             indexability_new,
             indexability_old) %>%
      as.data.frame()
    
    
    ## indexability status
    message("\tIndexability status\n")
    
    change_indexability_status <- identical_urls %>%
      filter(change_indexability_status == TRUE) %>%
      .[!.$address %in% change_status_code$address, ] %>%
      select(address,
             indexability_status_new,
             indexability_status_old) %>%
      as.data.frame()
    
    
    
    ## Get biggest ressources --------------------------
    message("~ Get biggest ressources\n")
    
    
    ## images
    message("\tImages")
    
    top_image <- crawl_new  %>%
      filter(str_detect(content, "image")) %>%
      select(address, size_bytes) %>%
      arrange(desc(size_bytes)) %>%
      as.data.frame()
    
    
    ## JavaScript
    message("\tJavaScript")
    
    top_js <- crawl_new  %>%
      filter(str_detect(content, "javascript")) %>%
      select(address, size_bytes) %>%
      arrange(desc(size_bytes)) %>% 
      as.data.frame()
    
    
    ## CSS
    message("\tCSS\n")
    
    top_css <- crawl_new  %>%
      filter(str_detect(content, "css")) %>%
      select(address, size_bytes) %>%
      arrange(desc(size_bytes)) %>%
      as.data.frame()
    
    
    
    ## Create Excel ------------------------------------
    message("~ Write Excel for: [ ", WEBSITE, " ] ", strrep("*", 40), "\n")
    
    ## set order of sheets here
    sheet_order <- c("new_urls",
                     "no_longer_linked_urls",
                     "change_orphan_pages",
                     "change_status_code",
                     "change_index",
                     "change_canonical",
                     # "change_indexability",
                     # "change_indexability_status",
                     "change_title",
                     "change_description",
                     "change_h1",
                     "change_word_count",
                     "change_content_type",
                     "change_crawl_depth",
                     "top_image",
                     "top_js",
                     "top_css")
    
    wb <- createWorkbook()
    
    
    for (sheet in sheet_order) {
      
      if (nrow(eval(parse(text = sheet))) > 0)
        
        add_sheets(wb, sheet)
      
    }
    
    
    saveWorkbook(wb, file.path(PATH_TO_XLSX_EXPORTS,
                               WEBSITE,
                               paste0(CRAWL_NEW_DATE,
                                      "__",
                                      CRAWL_OLD_DATE,
                                      "__",
                                      WEBSITE,
                                      ".xlsx")))
    
  }, error = function(e) {
    
    print(e)
    
    ## send Slack notification if error occured
    
    #fail_msg <- paste0(":siren: Script failed! Website: *",
    #                   WEBSITE,
    #                   "*\n\nError Message:")
    
    #text_slackr(fail_msg,
    #            preformatted = FALSE)
    
    #err_msg <- as.character(e)
    
    #text_slackr(err_msg,
    #            preformatted = TRUE)
    
    return(e)
    
  })
  
  ## send Slack notification if script was successful
  if(!inherits(try_res, "error")) {
  #  success_msg <- paste0(":aw_yeah: Script successful. Website: *",
  #                        WEBSITE,
  #                        "*")
    
  #  text_slackr(success_msg,
  #              preformatted = FALSE)
    
  }
  
}
