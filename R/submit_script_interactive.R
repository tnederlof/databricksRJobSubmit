#' Submit an R Script
#'
#' Transform an R script into a notebook task, installs R packages if needed,
#' and submits as job to a Databricks cluster.
#' @import shiny
#' @import fs
#' @import miniUI
#' @import brickster
#' @import rstudioapi
#' @import cli
#'
#' @export
submit_script_interactive <- function() {
  ui <- miniUI::miniPage(
    miniUI::gadgetTitleBar("Submit R Script to Databricks"),
    miniUI::miniContentPanel(
      miniUI::miniContentPanel(
        shiny::textInput("input_path", "Local R file path:"),
        shiny::textInput("cluster_id", "Databricks cluster id:"),
        shiny::actionButton("submit", "Submit Job")
      )
    )
  )

  server <- function(input, output, session) {

    shiny::observeEvent(input$submit, {

      result <- submit_script(input$input_path, input$cluster_id)

      # look up job details
      job_details <- db_jobs_runs_get(result$run_id)
      job_url <- job_details$run_page_url
      cli::cli_text("Databricks job started. Visit: {.url {job_url}}.")


      shiny::stopApp()
    })

  }

  shiny::runGadget(ui, server, viewer = shiny::dialogViewer("Submit R Job to Databrick", width = 1000, height = 800))
}


submit_script <- function(input_path, cluster_id) {
  # check env variables
  if (Sys.getenv("DATABRICKS_HOST") == "") {
    stop("Env variable DATABRICKS_HOST not found. Ensure your session is started with Databricks credentials: https://docs.posit.co/ide/server-pro/user/posit-workbench/managed-credentials/databricks.html")
  }
  if (Sys.getenv("DATABRICKS_CONFIG_FILE") == "") {
    stop("Env variable DATABRICKS_CONFIG_FILE not found. Ensure your session is started with Databricks credentials: https://docs.posit.co/ide/server-pro/user/posit-workbench/managed-credentials/databricks.html")
  }

  # check input_path
  input_path_split_vec <- fs::path_split(input_path)[[1]]
  input_filename_and_ext <- strsplit(input_path_split_vec[[length(input_path_split_vec)]], split ='\\.')[[1]]
  input_filename <- input_filename_and_ext[[1]]
  input_ext <- input_filename_and_ext[[2]]
  if (tolower(input_ext) != "r") {
    stop("A problem occured with the input path, please ensure its a file ending in 'r' or 'R'")
  }

  timestamp_chr <- as.character(as.numeric(as.POSIXct(Sys.time())) * 100000)
  current_user_info <- brickster::db_current_user()

  # check databricks_path
  databricks_path <- do.call(fs::path, list("/Workspace", "Users", current_user_info$userName))


  temp_dir_path <- fs::path_temp()
  new_filename <- paste0(input_filename, "_databricks_notebook_", timestamp_chr, ".R")
  new_filename_no_ext <- paste0(input_filename, "_databricks_notebook_", timestamp_chr)
  temp_path <- do.call(fs::path, list(temp_dir_path, new_filename))
  suppressWarnings(convert_r_notebook(input_path, temp_path))

  databricks_full_path <- do.call(fs::path, list(databricks_path, new_filename))

  import_result <- tryCatch(
    {
      return_code <- brickster::db_workspace_import(
        path = databricks_full_path,
        file = temp_path,
        format = "SOURCE",
        language = "R",
        overwrite = T
      )
      return_code
    },
    error = function(cond) {
      stop(paste("Error uploading notebook to the workspace: ", cond))
    },
    warning = function(cond) {
      stop(paste("Warning uploading notebook to the workspace: ", cond))
    }
  )

  run_name_used <- paste("Workbench Submitted Job", timestamp_chr)

  notebook_task <- brickster::job_task(
    task_key = timestamp_chr,
    existing_cluster_id = cluster_id,
    task = brickster::notebook_task(notebook_path = databricks_full_path)
  )

  job_result <- tryCatch(
    {
      job_submit_result <- brickster::db_jobs_runs_submit(
        tasks = brickster::job_tasks(notebook_task),
        run_name = run_name_used,
        idempotency_token = timestamp_chr
      )
      job_submit_result
    },
    error = function(cond) {
      stop(paste("Error submitting the job: ", cond))
    },
    warning = function(cond) {
      stop(paste("Warning submitting the job: ", cond))
    }
  )
  job_result
}

convert_r_notebook <- function(input_filename, output_filename) {
  code <- readLines(input_filename)
  final_code <- c(c("# Databricks notebook source", "# COMMAND ----------"), code)
  writeLines(final_code, file(output_filename))
}
