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
# author: Patrick Lürwer
# twitter: @netzstreuner
# date: 2017-11-10  
#
# 



# Info --------------------------------------------------------------------

# Die aus dem Screaming Frog exportierten Datei müssen nach folgendem Muster benannt werden:
# {Kunde}_{YYYY-MM-DD}_internal_all.csv
# bspw. gt_2017-12-31_internall_all.csv



# Libraries ---------------------------------------------------------------

library(tidyverse)
library(lubridate)
library(stringr)
library(xlsx)
library(httr)



# Setup -------------------------------------------------------------------

# Hier den Pfad zu den CSV-Exporten eintragen
path_to_crawls <- "C:/Pfad/zu/den/ScreamingFrog/Exporten/"

# Hier den Pfad eintragen, unter dem die Excel-Dateien gespeichert werden soll
path_to_export <- "C:/Pfad/zu/den/Excel/Exporten/"



# Funktion ----------------------------------------------------------------

get_status_code <- function(url) {
	
	out <- tryCatch(
		{
			HEAD(url, config(followlocation = FALSE)) %>%
				status_code()
		},
		error = function(cond) {
			message(paste("URL caused an error:", url))
			message(cond)
			return(NA)
		},
		warning = function(cond) {
			message(paste("URL caused a warning:", url))
			message(cond)
			return(NA)
		}
	)

	return(out)
	
}



# Kunden und Data ermitteln -----------------------------------------------

file_list <- list.files(path = path_to_crawls, pattern = ".csv")

for(file in file_list) {
	
	if (!exists("file_parts")) {
		file_parts <- tibble(customer = NA,
								date = NA,
								type = NA)
		
		file_parts <- file_parts %>% 
			mutate(path = file,
					customer = str_match(file_list[1], "^(.*?)_")[,2],
					date = ymd(str_match(file_list[1], "\\d{4}-\\d{2}-\\d{2}")[,1]),
					type = str_match(file_list[1], "(internal_all|all_inlinks)")[,2])  
		
	} else {
		temp_file_parts <- tibble(customer = NA,
									date = NA,
									type = NA)
		
		temp_file_parts <- temp_file_parts %>% 
			mutate(path = file,
					customer= str_match(file, "^(.*?)_")[,2],
					date = ymd(str_match(file, "\\d{4}-\\d{2}-\\d{2}")[,1]),
					type = str_match(file, "(internal_all|all_inlinks)")[,2])
		
		file_parts <- file_parts %>% 
			bind_rows(temp_file_parts)
		
		rm(temp_file_parts)
		
	}
}




# Überprüfen, ob für jeden Kunden zwei Crawls vorliegen -------------------

file_parts <- file_parts %>% 
	group_by(customer, type) %>% 
	filter(n() > 1)




# Ermitteln der letzten beiden Crawls je Kunden ---------------------------

files_internal_all <- file_parts %>%
	filter(type == "internal_all") %>% 
	group_by(customer) %>%
	mutate(date_rank = rank(desc(date))) %>% # rank dates for each customer
	filter(date_rank %in% c(1, 2)) # get the last two crawls for each customer based on the rank




# Einlesen der Crawls -----------------------------------------------------

for (i in 1:nrow(files_internal_all)) {
	
	if (!exists("internal_all")) {
		
		internal_all <- read_csv(str_c(path_to_crawls, files_internal_all$path[i], sep = "/"),
									skip = 1)
		
		internal_all <- internal_all %>% 
			mutate(customer = files_internal_all$customer[i],
					date = files_internal_all$date[i])
		
	} else {
		
		temp_internal_all <- read_csv(str_c(path_to_crawls, files_internal_all$path[i], sep = "/"),
										skip = 1)
		
		temp_internal_all <- temp_internal_all %>%
			mutate(customer = files_internal_all$customer[i],
					date = files_internal_all$date[i])
		
		internal_all <- internal_all %>% 
			bind_rows(temp_internal_all)
		
		rm(temp_internal_all)
		
	}
	
}




# Ein bisschen aufräumen -----------------------------------------------------------

# Spaltennamen der Crawls normalisieren

colnames(internal_all) <- internal_all %>% 
	colnames() %>% 
	str_replace_all("( |-)", "_") %>% 
	tolower()


# NAs durch "" ersetzen, um leere Zellen vergleichbar zu machen

internal_all[is.na(internal_all)] <- ""


# Nur HTML-Dateien verwenden

internal_all_html <- internal_all %>% 
	filter(str_detect(content, "text/html"))




# Abgleich der Crawls und Schreiben in Excel ------------------------------

customers_date <- files_internal_all %>%
	select(customer, date, date_rank) %>% 
	spread(key = "date_rank",
				 value = "date") %>%
	rename(crawl_new = `1`,
				 crawl_old = `2`)


for (i in 1:nrow(customers_date)) {
	
	# Aufteilen des alten und des neuen Crawls in separate Dataframes
	
	crawl_new <- internal_all_html %>% 
		filter(customer == customers_date$customer[i] & date == customers_date$crawl_new[i])
	
	crawl_old <- internal_all_html %>% 
		filter(customer == customers_date$customer[i] & date == customers_date$crawl_old[i])
	
	
	# Ermittlung der neuen URLs
	
	new_urls <- crawl_new %>% 
		anti_join(crawl_old, by = "address") %>%
		mutate(is_canonical = (address == canonical_link_element_1 | canonical_link_element_1 == "")) %>% 
		select(customer,
					 date,
					 address, 
					 status_code, 
					 meta_robots_1, 
					 canonical_link_element_1, 
					 title_1, 
					 meta_description_1,
					 is_canonical) %>% 
		mutate(date = as.character(date)) %>% 
		as.data.frame()
	
	
	# Ermittlung der nicht mehr gefundenen URLs
	
	deleted_urls <- crawl_old %>% 
		anti_join(crawl_new, by = "address") %>%
		mutate(status_code_current = map_int(address, get_status_code)) %>% 
		select(customer,
					 date,
					 address, 
					 status_code,
					 status_code_current,
					 meta_robots_1, 
					 canonical_link_element_1, 
					 title_1, 
					 meta_description_1) %>%
		rename(status_code_old = status_code) %>% 
		mutate(date = as.character(date)) %>% 
		as.data.frame()
	
	
	# Ermittlung der identischen URLs
	
	identical_urls <- crawl_new %>% 
		inner_join(crawl_old, by = "address", suffix = c("_new", "_old")) %>%
		mutate_at(c("meta_robots_1_new", "meta_robots_1_old", "content_new", "content_old"),
							tolower) %>% 
		mutate_at(c("meta_robots_1_new", "meta_robots_1_old", "content_new", "content_old"),
							str_replace_all, pattern = "\\s", replacement = "") %>% 
		mutate(status_code_change = status_code_new != status_code_old,
					 canonical_change = canonical_link_element_1_new != canonical_link_element_1_old,
					 index_change = meta_robots_1_new != meta_robots_1_old,
					 title_change = title_1_new != title_1_old,
					 description_change = meta_description_1_new != meta_description_1_old,
					 content_type_change = content_new != content_old,
					 crawl_depth_change = crawl_depth_new != crawl_depth_old,
					 h1_change = h1_1_new != h1_1_old,
					 size_change = round(abs(as.numeric(size_new) / as.numeric(size_old) - 1),2) >= 0.1,
					 response_time_change = round(abs(as.numeric(response_time_new) / as.numeric(response_time_old) - 1),2) >= 0.1) 
	
	
	# Ermittlung der identischen URLs mit Änderungen
	
	## Status Code
	status_code_change <- identical_urls %>%
		filter(status_code_change == TRUE) %>% 
		select(address,
					 status_code_new,
					 status_code_old) %>% 
		as.data.frame()

	## Canonical Link
	canonical_change <- identical_urls %>% 
		filter(canonical_change == TRUE) %>%
		.[!.$address %in% status_code_change$address, ] %>%  
		select(address,
					 canonical_link_element_1_new,
					 canonical_link_element_1_old) %>%
		mutate(new_is_canonical = (address == canonical_link_element_1_new | canonical_link_element_1_new ==""),
					 old_was_canonical = (address == canonical_link_element_1_old | canonical_link_element_1_old == "")) %>%
		as.data.frame()

	## Meta Robots
	index_change <- identical_urls %>% 
		filter(index_change == TRUE) %>%
		.[!.$address %in% status_code_change$address, ] %>%
		select(address, 
					 meta_robots_1_new, 
					 meta_robots_1_old) %>%
		as.data.frame()
	
	## Title
	title_change <- identical_urls %>% 
		filter(title_change == TRUE) %>%
		.[!.$address %in% status_code_change$address, ] %>%
		select(address, 
					 title_1_new, 
					 title_1_old) %>%
		as.data.frame()
	
	## Description
	description_change <- identical_urls %>% 
		filter(description_change == TRUE) %>%
		.[!.$address %in% status_code_change$address, ] %>%
		select(address, 
					 meta_description_1_new, 
					 meta_description_1_old) %>%
		as.data.frame()
	
	## Content Type
	content_type_change <- identical_urls %>% 
		filter(content_type_change == TRUE) %>% 
		.[!.$address %in% status_code_change$address, ] %>%
		select(address,
					 content_new,
					 content_old) %>% 
		as.data.frame()
	
	## Klicktiefe
	crawl_depth_change <- identical_urls %>% 
		filter(crawl_depth_change == TRUE) %>% 
		.[!.$address %in% status_code_change$address, ] %>%
		select(address,
					 crawl_depth_new,
					 crawl_depth_old) %>% 
		as.data.frame()
	
	## H1
	h1_change <- identical_urls %>% 
		filter(h1_change == TRUE) %>% 
		.[!.$address %in% status_code_change$address, ] %>%
		select(address,
					 h1_1_new,
					 h1_1_old) %>% 
		as.data.frame()
	
	## Dateigröße	
	size_change <- identical_urls %>% 
		filter(size_change == TRUE) %>% 
		.[!.$address %in% status_code_change$address, ] %>%
		select(address,
					 size_new,
					 size_old) %>% 
		as.data.frame()
	
	## Response Time	
	response_time_change <- identical_urls %>% 
		filter(response_time_change == TRUE) %>% 
		.[!.$address %in% status_code_change$address, ] %>%
		select(address,
					 response_time_new,
					 response_time_old) %>% 
		as.data.frame()
	
	
	
	
	# Schreiben in Excel
	
	wb <- createWorkbook()
	
	sheet_new_urls <- createSheet(wb, "new_urls")
	addDataFrame(new_urls, 
							 sheet = sheet_new_urls, 
							 row.names = FALSE)
	
	sheet_deleted_urls <- createSheet(wb, "deleted_urls")
	addDataFrame(deleted_urls, 
							 sheet = sheet_deleted_urls, 
							 row.names = FALSE)
	
	sheet_status_code_change <- createSheet(wb, "status_code_change")
	addDataFrame(status_code_change,
							 sheet = sheet_status_code_change,
							 row.names = FALSE)
	
	sheet_canonical_change <- createSheet(wb, "canonical_change")
	addDataFrame(canonical_change, 
							 sheet = sheet_canonical_change, 
							 row.names = FALSE)
	
	sheet_index_change <- createSheet(wb, "index_change")
	addDataFrame(index_change, 
							 sheet = sheet_index_change, 
							 row.names = FALSE)
	
	sheet_title_change <- createSheet(wb, "title_change")
	addDataFrame(title_change, 
							 sheet = sheet_title_change, 
							 row.names = FALSE)
	
	sheet_description_change <- createSheet(wb, "description_change")
	addDataFrame(description_change, 
							 sheet = sheet_description_change, 
							 row.names = FALSE)
	
	sheet_content_type_change <- createSheet(wb, "content_type_change")
	addDataFrame(content_type_change, 
							 sheet = sheet_content_type_change, 
							 row.names = FALSE)
	
	sheet_crawl_depth_change <- createSheet(wb, "crawl_depth_change")
	addDataFrame(crawl_depth_change, 
							 sheet = sheet_crawl_depth_change, 
							 row.names = FALSE)
	
	sheet_h1_change <- createSheet(wb, "h1_change")
	addDataFrame(h1_change, 
							 sheet = sheet_h1_change, 
							 row.names = FALSE)
	
	sheet_size_change <- createSheet(wb, "size_change")
	addDataFrame(size_change, 
							 sheet = sheet_size_change, 
							 row.names = FALSE)
	
	sheet_response_time_change <- createSheet(wb, "response_time_change")
	addDataFrame(response_time_change, 
							 sheet = sheet_response_time_change, 
							 row.names = FALSE)
	
	
	saveWorkbook(wb, paste0(path_to_export, 
													customers_date$crawl_new[i], 
													"__", 
													customers_date$crawl_old[i], 
													"__", 
													customers_date$customer[i], 
													".xlsx"))
	
	print(paste0("Excel written: ",
							 customers_date$crawl_new[i], 
							 "__", 
							 customers_date$crawl_old[i], 
							 "__", 
							 customers_date$customer[i], 
							 ".xlsx"))

}


























