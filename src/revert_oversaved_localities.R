library(readr)
library(purrr)
library(dplyr)

###################READ DATA####################################################
#Easily found in code history on Github
newest <- read_csv("./data/input/localities_10-06.csv")

wrong <- read_csv("./data/input/localities_oldest_reverted.csv") %>%
  select(-geometry.7)

correct <- read_csv("./data/input/localities_correct_oldest.csv") %>%
  select(-geometry.7)


##################IDENTIFY DIFFERENCES##########################################
incorrect_changes <- correct %>%
  full_join(wrong, by = "locID", suffix = c("_correct", "_wrong"))

# Identify rows where there are differences between correct and wrong

incorrect_changes <- incorrect_changes %>%
  filter(
    rowSums(select(., ends_with("_correct")) != select(., ends_with("_wrong")), na.rm = TRUE) > 0
  ) %>%
  select(locID, ends_with("_correct"), ends_with("_wrong"))

print("Incorrect changes between correct and wrong:")
print(incorrect_changes)


# Join wrong and newest tables on locID
genuine_changes <- wrong %>%
  full_join(newest, by = "locID", suffix = c("_wrong", "_newest"))

# Identify rows where there are differences between wrong and newest
genuine_changes <- genuine_changes %>%
  filter(
    rowSums(select(., ends_with("_wrong")) != select(., ends_with("_newest")), na.rm = TRUE) > 0
  ) %>%
  select(locID, ends_with("_wrong"), ends_with("_newest"))

print("Genuine changes between wrong and newest:")
print(genuine_changes)

####################IDENTIFY NEW LOCIDs#########################################

# Look for locIDs in newest not yet in wrong (new entries)
# Identify new locIDs in newest that are not in wrong
new_locIDs <- setdiff(newest$locID, wrong$locID)

# Create a table for new locID entries
genuine_new_entries <- newest %>%
  filter(locID %in% new_locIDs)

# Loop over each genuine change and update the final_table

final_table <- correct %>% 
  rbind(genuine_new_entries)

# Note this might generate duplicates, i.e. records in correct but not yet in wrong,
# check later and take latest status (see last part of script)

for (i in 1:nrow(genuine_changes)) {
  loc_id <- genuine_changes$locID[i]
  update_values <- genuine_changes[i, grep("_newest", names(genuine_changes))]
  update_values <- update_values %>%
    setNames(gsub("_newest", "", names(update_values)))
  
  # Update the corresponding row in the final_table
  final_table[final_table$locID == loc_id, names(update_values)] <- update_values
}

####################HANDLING NEW LOCIDS#########################################
####################CHECKS######################################################
# How many locIDs in each table?
length(unique(wrong$locID))
length(unique(correct$locID))
length(unique(newest$locID))
length(unique(final_table$locID))


# Identify differing locIDs
diff_correct_wrong <- setdiff(correct$locID, wrong$locID)
diff_wrong_correct <- setdiff(wrong$locID, correct$locID)

diff_wrong_newest <- setdiff(wrong$locID, newest$locID)
diff_newest_wrong <- setdiff(newest$locID, wrong$locID)

diff_correct_newest <- setdiff(correct$locID, newest$locID)
diff_newest_correct <- setdiff(newest$locID, correct$locID)

diff_correct_final <- setdiff(correct$locID, final_table$locID)
diff_final_correct <- setdiff(final_table$locID, correct$locID)

diff_wrong_final <- setdiff(wrong$locID, final_table$locID)
diff_final_wrong <- setdiff(final_table$locID, wrong$locID)

diff_newest_final <- setdiff(newest$locID, final_table$locID)
diff_final_newest <- setdiff(final_table$locID, newest$locID)

# Print differing locIDs
cat("LocIDs in correct but not in wrong:", diff_correct_wrong, "\n")
cat("LocIDs in wrong but not in correct:", diff_wrong_correct, "\n")

cat("LocIDs in wrong but not in newest:", diff_wrong_newest, "\n")
cat("LocIDs in newest but not in wrong:", diff_newest_wrong, "\n")

cat("LocIDs in correct but not in newest:", diff_correct_newest, "\n")
cat("LocIDs in newest but not in correct:", diff_newest_correct, "\n")

cat("LocIDs in correct but not in final_table:", diff_correct_final, "\n")
cat("LocIDs in final_table but not in correct:", diff_final_correct, "\n")

cat("LocIDs in wrong but not in final_table:", diff_wrong_final, "\n")
cat("LocIDs in final_table but not in wrong:", diff_final_wrong, "\n")

cat("LocIDs in newest but not in final_table:", diff_newest_final, "\n")
cat("LocIDs in final_table but not in newest:", diff_final_newest, "\n")



#Check duplicates (the ones occurring already in correct, but update in newest, we need to keep most recent)

# Find duplicate locIDs in the final_table
duplicate_locIDs <- final_table$locID[duplicated(final_table$locID) | duplicated(final_table$locID, fromLast = TRUE)]

# Extract duplicate entries from final_table
duplicates_final_table <- final_table %>%
  filter(locID %in% duplicate_locIDs)

# Print the duplicate locID entries
print("Duplicate entries in final_table:")
print(duplicates_final_table)

# Print unique duplicate locIDs
unique_duplicate_locIDs <- unique(duplicates_final_table$locID)
cat("Duplicate locIDs in final_table:", unique_duplicate_locIDs, "\n")

# Filter duplicates_final_table to keep only the entries that match the entries in newest
filtered_duplicates_final_table <- duplicates_final_table %>%
  semi_join(newest, by = colnames(newest))

# Print the filtered duplicate entries
print("Filtered duplicate entries in final_table:")
print(filtered_duplicates_final_table)

# Remove the old duplicates from final_table and add the filtered duplicates
filtered_final_table <- final_table %>%
  filter(!(locID %in% duplicate_locIDs)) %>%
  bind_rows(filtered_duplicates_final_table)

# Print the final filtered table
print("Final filtered table:")
print(filtered_final_table)

