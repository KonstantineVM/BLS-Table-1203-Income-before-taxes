# install and load the readr package if not already installed
if (!require(readr)) {
  install.packages("readr")
}
if (!require(reshape2)) {
  install.packages("reshape2")
}

library(readr)
library(dplyr)
library(purrr)
library(reshape2)
library(sqldf)
library(tidyverse)
library(knitr)

# Specify the year
# This is the year of the data we are working with.
year <- 2021

# Specify the location of the unzipped microdata file
# This is the directory where the data files are stored.
drive <- "input"

#########################################################################
# This step involves three sub-steps:
# 1. Convert the stub parameter file into a label file for output
#    This process will make it easier to understand the output data by 
#    attaching meaningful labels to the data.
# 2. Convert the stub parameter file into an expenditure aggregation file
#    This process will aggregate the expenditure data at some level (to be 
#    specified in the code that performs this task).
# 3. Create formats for use in other procedures
#    This process will create data formats that can be used in subsequent 
#    data processing tasks.
#########################################################################

# Calculate YR1 and YR2
# These are the last two digits of the current year and the next year, 
# respectively. They may be used in file naming or data filtering.
YR1 <- substr(year, start = 3, stop = 4)
YR2 <- substr(year + 1, start = 3, stop = 4)

# Define the directory for a specific year
# In SAS, a libname is a shortcut or logical name assigned to a physical location.
# In R, we just use the directory path directly. So, we create a variable for the directory.
i_yr1_directory <- paste0(drive, "/INTRVW", YR1)


# Let's assume that the variable YEAR is defined somewhere earlier in your code
YEAR <- "2021"

# String manipulation to get YR1 and YR2
YR1 <- substr(YEAR, 3, 4)
YR2 <- substr(as.character(as.numeric(YEAR) + 1), 3, 4)

# Read in the text file
file_path <- paste0("input/stubs/CE-HG-Inter-", year, ".TXT")
stubfile <- read.fwf(file_path, 
                 widths = c(3, 3, 63, 10, 6, 9), 
                 col.names = c("TYPE", "LEVEL", "TITLE", "UCC", "SURVEY", "GROUP"), 
                 stringsAsFactors = FALSE)

# Remove leading and/or trailing whitespace from character strings
stubfile <- data.frame(lapply(stubfile, function(x) {
  if(is.character(x)) return(trimws(x)) 
  else return(x)
}))

# Filter the dataset
stubfile <- stubfile[stubfile$TYPE == '1' & stubfile$GROUP %in% c('CUCHARS', 'FOOD', 'EXPEND', 'INCOME'),]

# Add line numbers
stubfile <- mutate(stubfile, COUNT = row_number() + 9999)
stubfile <- mutate(stubfile, LINE = paste0(COUNT, LEVEL))

# Create new data frame AGGFMT1 and initialize new columns and MAPS LINE NUMBERS TO UCCS
AGGFMT1 <- stubfile %>%
  dplyr::select("UCC", "LINE") %>%
  mutate(LINE1 = NA_character_, 
         LINE2 = NA_character_,
         LINE3 = NA_character_,
         LINE4 = NA_character_,
         LINE5 = NA_character_,
         LINE6 = NA_character_,
         LINE7 = NA_character_,
         LINE8 = NA_character_,
         LINE9 = NA_character_,
         LINE10 = NA_character_)

# Fill the LINES array based on the condition
LINES <- c(rep(NA, 9))
LINES <- as.list(LINES)
for(i in 1:nrow(AGGFMT1)) {
  if(AGGFMT1$UCC[i] > 'A') {
    line_num <- as.numeric(substr(AGGFMT1$LINE[i], 6, 6)) # get the 6th character of LINE as a number
    LINES[line_num] <- AGGFMT1$LINE[i]
    # Create a vector of column names
    column_names <- paste0("LINE", 1:9)
    }
  # Assign values to the columns in AGGFMT1
  AGGFMT1[i, column_names] <- unlist(LINES)
  if(AGGFMT1$UCC[i] < 'A') {
    AGGFMT1$LINE10[i] <- AGGFMT1$LINE[i]
  }
}

# Filter rows where LINE10 is not NA
AGGFMT1 <- AGGFMT1[!is.na(AGGFMT1$LINE10),]

# Rename the 'LINE' column to 'COMPARE' and sort the data frame by 'UCC'
AGGFMT1 <- AGGFMT1 %>%
  rename(COMPARE = LINE) %>%
  arrange(UCC)

# Transpose the data
AGGFMT2 <- AGGFMT1 %>%
  gather(key = "variable", value = "LINE", LINE1:LINE10) %>%
  arrange(UCC, COMPARE)

# Filter the data and keep only the necessary columns
AGGFMT <- AGGFMT2 %>%
  filter(!is.na(LINE), 
         as.numeric(str_sub(COMPARE, 6, 6)) > as.numeric(str_sub(LINE, 6, 6)) | COMPARE == LINE) %>%
  dplyr::select(UCC, LINE)

# Perform the SQL query
result <- sqldf("SELECT UCC, LINE FROM AGGFMT")

# Store the results into vectors and count the total rows
UCCS <- paste(result$UCC, collapse = " ")
LINES <- paste(result$LINE, collapse = " ")
CNT <- nrow(result)

# Split UCCS and LINES into vectors
UCCS_vector <- strsplit(UCCS, " ")[[1]]
LINES_vector <- strsplit(LINES, " ")[[1]]

# Create a named vector
mapping <- setNames(LINES_vector, UCCS_vector)

# Filter and rename the data, and add new columns
LBLFMT <- stubfile %>%
  dplyr::select(LINE, TITLE) %>%
  rename(START = LINE, LABEL = TITLE) %>%
  mutate(START = as.numeric(START)) %>%
  mutate(FMTNAME = 'LBLFMT', TYPE = 'C')

# Create AGGFMT function
AGGFMT <- function(x) {
  ifelse(is.na(mapping[x]), 'OTHER', mapping[x])
}

# Create INC function
INC <- function(x) {
  map <- c('01' = '01', '02' = '02', '03' = '03', '04' = '04', '05' = '05',
           '06' = '06', '07' = '07', '08' = '08', '09' = '09', '10' = '10')
  ifelse(is.na(map[x]), '10', map[x])
}

# Create a named vector for the label format
lblfmt_vector <- setNames(as.character(LBLFMT$LABEL), LBLFMT$START)

# Define the label format function
lblfmt <- function(x) {
  ifelse(is.na(lblfmt_vector[x]), x, lblfmt_vector[x])
}

###########################################################################
# STEP2: READ IN ALL NEEDED DATA                                          */
# ----------------------------------------------------------------------- */
# 1 READ IN THE INTERVIEW FMLY FILES & CREATE THE MO_SCOPE VARIABLE       */
# 2 READ IN THE INTERVIEW MTAB AND ITAB FILES                             */
# 3 MERGE FMLY AND EXPENDITURE FILES TO DERIVE WEIGHTED EXPENDITURES      */
###########################################################################

# Load the necessary libraries
library(dplyr)
library(data.table)

# Set the data directory
data_dir <- "input/intrvw21"

# Set the paths to the input files
files <- sprintf("%s/fmli%s%d.csv", data_dir, YR1, 2:4)
files <- c(files, sprintf("%s/fmli%s1.csv", data_dir, YR2))

# Define the columns to keep
cols_to_keep <- c("NEWID", paste0("WTREP", sprintf("%02d", 1:44)), "FINLWT21", "QINTRVMO", "FINCBTXM")

# Read in FMLY file data, keeping only necessary columns
FMLY <- rbindlist(lapply(files, function(x) {
  df <- fread(x, select = cols_to_keep)
  df$LASTQTR <- grepl(sprintf("fmli%s1", YR2), x)
  df
}))

# Create MONTH IN SCOPE variable (MO_SCOPE)
FMLY <- FMLY %>%
  mutate(MO_SCOPE = ifelse(LASTQTR, 4 - QINTRVMO, 3))

# Adjust weights by MO_SCOPE to account for sample rotation
# Get column names
REPS_A_cols <- c(paste0("WTREP", sprintf("%02d", 1:44)), "FINLWT21")
REPS_B_cols <- paste0("REPWT", sprintf("%02d", 1:45))

for (i in 1:45) {
  FMLY[[REPS_B_cols[i]]] <- ifelse(FMLY[[REPS_A_cols[i]]] > 0, 
                                   FMLY[[REPS_A_cols[i]]] * FMLY$MO_SCOPE / 12, 
                                   0)
}


# Create variable INCLASS for interview survey
FMLY <- FMLY %>%
  mutate(INCLASS = case_when(
    FINCBTXM < 15000 ~ '01',
    FINCBTXM >= 15000 & FINCBTXM < 30000 ~ '02',
    FINCBTXM >= 30000 & FINCBTXM < 40000 ~ '03',
    FINCBTXM >= 40000 & FINCBTXM < 50000 ~ '04',
    FINCBTXM >= 50000 & FINCBTXM < 70000 ~ '05',
    FINCBTXM >= 70000 & FINCBTXM < 100000 ~ '06',
    FINCBTXM >= 100000 & FINCBTXM < 150000 ~ '07',
    FINCBTXM >= 150000 & FINCBTXM < 200000 ~ '08',
    FINCBTXM >= 200000 ~ '09'
  ))

# Keep only necessary variables
FMLY <- FMLY %>%
  dplyr::select(NEWID, INCLASS, starts_with("WTREP"), FINLWT21, starts_with("REPWT"))


# Set the paths to the input files
files_MTBI <- sprintf("%s/mtbi%s%d.csv", data_dir, YR1, 2:4)
files_MTBI <- c(files_MTBI, sprintf("%s/mtbi%s1.csv", data_dir, YR2))

files_ITBI <- sprintf("%s/itbi%s%d.csv", data_dir, YR1, 2:4)
files_ITBI <- c(files_ITBI, sprintf("%s/itbi%s1.csv", data_dir, YR2))

# Read in the data, renaming 'VALUE' column to 'COST' for ITBI files
data_MTBI <- rbindlist(lapply(files_MTBI, fread, select = c("NEWID", "UCC", "COST", "REF_YR"), colClasses = list(character = "UCC")))
data_ITBI <- rbindlist(lapply(files_ITBI, fread, select = c("NEWID", "UCC", "VALUE", "REFYR"), col.names = c("NEWID", "UCC", "COST", "REF_YR"), colClasses = list(character = "UCC")))

# Bind rows together
EXPEND <- rbind(data_MTBI, data_ITBI)

# Keep only rows where REFYR or REF_YR is equal to YEAR
EXPEND <- EXPEND[EXPEND$REF_YR == year, ]

# Adjust COST for UCC 710110
EXPEND$COST[EXPEND$UCC == '710110'] <- EXPEND$COST[EXPEND$UCC == '710110'] * 4
# Sort EXPEND by NEWID
EXPEND <- EXPEND[order(EXPEND$NEWID), ]

# Make sure that 'NEWID' is of the same type in both data frames
FMLY$NEWID <- as.character(FMLY$NEWID)
EXPEND$NEWID <- as.character(EXPEND$NEWID)

# Add a source column to each dataset
FMLY$INFAM <- TRUE
EXPEND$INEXP <- TRUE

# Merge FMLY and EXPEND by NEWID
PUBFILE <- merge(FMLY, EXPEND, by = "NEWID", all = TRUE)

# If COST is NA, set it to 0
PUBFILE$COST[is.na(PUBFILE$COST)] <- 0

# Add RCOST1-RCOST45 columns to PUBFILE and initialize them with 0
for (i in 1:45) {
  PUBFILE[[paste0("RCOST", i)]] <- 0
}

# Multiply COSTS by WEIGHTS to derive WEIGHTED COSTS
cols_REPS_A <- c(paste0("WTREP", sprintf("%02d", 1:44)), "FINLWT21")
cols_REPS_B <- paste0("RCOST", 1:45)
for (i in 1:45) {
  PUBFILE[[cols_REPS_A[i]]] <- ifelse(is.na(PUBFILE[[cols_REPS_A[i]]]),
                                      0,
                                      PUBFILE[[cols_REPS_A[i]]])
}
for (i in 1:45) {
  PUBFILE[[cols_REPS_B[i]]] <- ifelse(PUBFILE[[cols_REPS_A[i]]] > 0, 
                                      PUBFILE[[cols_REPS_A[i]]] * PUBFILE$COST, 
                                      0)
}

# Keep only rows where both INEXP and INFAM are TRUE
PUBFILE <- PUBFILE[!is.na(PUBFILE$INFAM) & !is.na(PUBFILE$INEXP), ]

# Select necessary columns
PUBFILE <- subset(PUBFILE, select=c("NEWID", "INCLASS", "UCC", cols_REPS_B))

############################################################################
#* STEP3: CALCULATE POPULATIONS                                            *#
#* ----------------------------------------------------------------------- *#
#* 1 SUM ALL 45 WEIGHT VARIABLES TO DERIVE REPLICATE POPULATIONS           *#
#* 2 FORMAT FOR CORRECT COLUMN CLASSIFICATIONS                             *#
############################################################################

# Create a named vector to map 'INCLASS' values
INC <- c('01', '01', '02', '02', '03', '03', '04', '04', '05', '05', '06', '06', '07', '07', '08', '08', '09', '09')
names(INC) <- c('01', '10', '02', '10', '03', '10', '04', '10', '05', '10', '06', '10', '07', '10', '08', '10', '09', '10')

# Apply the mapping to the 'INCLASS' column
FMLY$INCLASS <- INC[as.character(FMLY$INCLASS)]

# Create a list of column names for the variables to sum
columns_to_sum <- c(paste0("REPWT", sprintf("%02d", 1:45)))

# Sum the variables by 'INCLASS'
POP <- FMLY %>%
  group_by(INCLASS) %>%
  summarize(across(all_of(columns_to_sum), sum, na.rm = TRUE), .groups = "drop")

# Rename the columns
colnames(POP) <- c("INCLASS", paste0("RPOP", 1:45))

# Define the groups that should also be labeled as '10'
groups <- c('01', '02', '03', '04', '05', '06', '07', '08', '09')

# Create a new row with 'INCLASS' equal to '10' and the sums of the specified groups
new_row <- POP %>% 
  filter(INCLASS %in% groups) %>% 
  summarise(across(-INCLASS, sum, na.rm = TRUE)) # Exclude 'INCLASS'

# Set 'INCLASS' to '10' for the new row
new_row$INCLASS <- '10'

# Add the new row to the POP data frame
POP <- rbind(POP, new_row)

############################################################################
#* STEP4: CALCULATE WEIGHTED AGGREGATE EXPENDITURES                        *#
#* ----------------------------------------------------------------------- *#
#* 1 SUM THE 45 REPLICATE WEIGHTED EXPENDITURES TO DERIVE AGGREGATES       *#
#* 2 FORMAT FOR CORRECT COLUMN CLASSIFICATIONS AND AGGREGATION SCHEME      *#
############################################################################

library(dplyr)
library(purrr)

# Install the package if you haven't already
if (!require(pbapply)) {
  install.packages("pbapply")
}

# Use pblapply instead of lapply to include a progress bar
library(pbapply)

# Create a list of data frames, one for each LINE value
df_list <- pblapply(unique(mapping), function(line) {
  ucc_values <- names(mapping[mapping == line])
  df <- subset(PUBFILE, UCC %in% ucc_values)
  df$LINE <- line  # Replace UCC with LINE
  # Ensure all income groups are included
  df <- df %>%
    complete(LINE, INCLASS = factor(INCLASS, levels = c('01', '02', '03', '04', '05', '06', '07', '08', '09')), fill = list(count = 0))
  df
})

# Perform the aggregation on each data frame in the list
AGG_list <- pblapply(df_list, function(df) {
  df %>%
    mutate(INCLASS = INC[INCLASS]) %>%
    group_by(LINE, INCLASS) %>%
    summarise(across(starts_with("RCOST"), sum, na.rm = TRUE), .groups = "drop")
})

# Convert UCC to character in all data frames
AGG_list <- lapply(AGG_list, function(df) {
  df$LINE <- as.character(df$LINE)
  return(df)
})

# Now bind all data frames
AGG <- bind_rows(AGG_list)

# Add the '10' group
new_rows <- pblapply(df_list, function(df) {
  df %>%
    mutate(INCLASS = INC[INCLASS]) %>%
    group_by(LINE) %>%
    summarise(across(starts_with("RCOST"), sum, na.rm = TRUE), .groups = "drop") %>%
    mutate(INCLASS = '10')
})

# Combine all the new rows into a single data frame
new_rows_df <- do.call(rbind, new_rows)

# Add the new rows to the AGG data frame
AGG <- rbind(AGG, new_rows_df)

  ###########################################################################
  # STEP5: CALCULTATE MEAN EXPENDITURES                                     */
  # ----------------------------------------------------------------------- */
  # 1 READ IN POPULATIONS AND LOAD INTO MEMORY USING A 2 DIMENSIONAL ARRAY  */
  #   POPULATIONS ARE ASSOCIATED BY INCLASS(i), AND REPLICATE(j)            */
  # 2 READ IN AGGREGATE EXPENDITURES FROM AGG DATASET                       */
  #   CALCULATE MEANS BY DIVIDING AGGREGATES BY CORRECT SOURCE POPULATIONS  */
  # 4 CALCULATE STANDARD ERRORS USING REPLICATE FORMULA                     */
  ###########################################################################

pop_list <- split(POP, f = POP$INCLASS)  # Split the pop dataframe into a list of dataframes, one for each INCLASS
pop_list <- lapply(pop_list, function(df) {
  as.numeric(unlist(df[-1]))  # Exclude the 'INCLASS' column and convert to numeric vector
})

agg <- AGG %>%
  rowwise() %>%
  mutate(across(starts_with("RCOST"), 
                ~ . / pop_list[[INCLASS]][[as.numeric(str_remove(cur_column(), "RCOST"))]], 
                .names = "MEAN{.col}"))  # Calculate the means

agg$MEAN <- agg$MEANRCOST45

# Calculate the squared differences
for(i in 1:44) {
  agg[[paste0("DIFF", i)]] <- (agg[[paste0("MEANRCOST", i)]] - agg$MEAN)^2
}

# Calculate the standard error
agg$SE <- sqrt(rowSums(agg[, grep("DIFF", names(agg))]) / 44)

# Select only needed columns
TAB1 <- agg %>% 
  dplyr::select(LINE, MEAN, SE)

  ###########################################################################
  # STEP6: TABULATE EXPENDITURES                                            */
  # ----------------------------------------------------------------------- */
  # 1 ARRANGE DATA INTO TABULAR FORM                                        */
  # 2 SET OUT INTERVIEW POPULATIONS FOR POPULATION LINE ITEM                */
  # 3 INSERT POPULATION LINE INTO TABLE                                     */
  # 4 INSERT ZERO EXPENDITURE LINE ITEMS INTO TABLE FOR COMPLETENESS        */
  ##########################################################################

# Convert to long format and add a measurement index
TAB1_long <- TAB1 %>%
  pivot_longer(cols = c(MEAN, SE), names_to = "ESTIMATE", values_to = "variable") %>%
  group_by(LINE, ESTIMATE) %>%
  mutate(measurement_index = row_number())

# Convert back to wide format
TAB2 <- TAB1_long %>%
  pivot_wider(names_from = measurement_index, values_from = variable)

# Rename the columns
TAB2 <- TAB2 %>%
  rename_with(~paste0("INCLASS", .), .cols = -c(LINE, ESTIMATE))

CUS <- data.frame(
  LINE = "RPOP45",
  matrix(POP$RPOP45, nrow = 1, dimnames = list(NULL, paste0("INCLASS", 1:length(POP$RPOP45))))
)

# Merge CUS and TAB2
TAB3 <- bind_rows(CUS, TAB2)

# Update LINE and ESTIMATE if LINE == 'RPOP45'
TAB3 <- TAB3 %>%
  mutate(LINE = ifelse(LINE == 'RPOP45', '100001', LINE),
         ESTIMATE = ifelse(LINE == '100001', 'N', ESTIMATE))

# Merge TAB3 and STUBFILE
TAB <- full_join(TAB3, stubfile, by = "LINE")

# Delete rows where SURVEY is 'S' and LINE is not '100001'
TAB <- TAB %>%
  filter(!(LINE != '100001' & SURVEY == 'S'))

# Replace missing values in INCLASS1 to INCLASS10 with 0
TAB[paste0("INCLASS", 1:10)] <- lapply(TAB[paste0("INCLASS", 1:10)], function(x) ifelse(is.na(x), 0, x))

# Set ESTIMATE to 'MEAN' if all values in INCLASS1 to INCLASS10 are 0
TAB <- TAB %>%
  mutate(ESTIMATE = ifelse(rowSums(select(., starts_with("INCLASS"))) == 0, 'MEAN', ESTIMATE))

# Delete rows where GROUP is 'CUCHARS' or 'INCOME' and LINE is the same as the previous row
TAB <- TAB %>%
  filter(!(GROUP %in% c('CUCHARS', 'INCOME') & LINE == lag(LINE)))

# Delete rows where TITLE is "Percent distribution:"
TAB <- TAB %>%
  filter(TITLE != "Percent distribution:")

# Delete rows where SURVEY is 'T'
TAB <- TAB %>%
  filter(SURVEY != 'T')


# Filter the data
TAB <- TAB %>%
  filter(LINE != 'OTHER')

# Perform the tabulation
TAB_SUM <- TAB %>%
  group_by(LINE, ESTIMATE) %>%
  summarise(across(starts_with("INCLASS"), sum, na.rm = TRUE)) %>%
  mutate(across(starts_with("INCLASS"), ~ifelse(is.na(.), 0, .)))

# Add labels
names(TAB_SUM)[names(TAB_SUM) %in% paste0("INCLASS", 1:10)] <- c('LESS THAN $15,000', '$15,000 TO $29,999', 
                                                                 '$30,000 TO $39,999', '$40,000 TO $49,999',
                                                                 '$50,000 TO $69,999', '$70,000 TO $99,999',
                                                                 '$100,000 TO $149,999', '$150,000 TO $199,999',
                                                                 '$200,000 AND OVER', 'ALL CONSUMER UNITS')

# Apply the mapping to LINE
TAB_SUM <- TAB_SUM %>%
  mutate(LINE = as.numeric(LINE)) %>%
  left_join(LBLFMT, by = c("LINE" = "START")) %>%
  mutate(LINE = ifelse(is.na(LABEL), LINE, LABEL)) %>%
  select(-LABEL)

