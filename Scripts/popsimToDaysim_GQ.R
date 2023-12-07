#' Prepares Household and person file inputs for DAYSIM using outputs from PopSim Population Synthesizer
#' author: Khademul Haque, RSG, khademul.haque@rsginc.com
#' April 2018
#' This script written in R and performs the following tasks:
#' - Reads in PopSim output (exported synthetic population) at the MAZ level
#' - Reads in raw ACS PUMS sample data
#' - Create correspondence between ids in PopSim synthetic population and PUMS ids
#' - Merges PUMS information into the synthetic population and calculates all variables required for DaySim household and person files
#' How to Run: 
#' 1. Drop the following input files in the 'data' folder with the exact naming convention:
#'    1.1 Household file from PopulationSim output (synthetic_households.csv)
#'    1.2 Person file from PopulationSim output (synthetic_persons.csv)
#'    1.3 ACS PUMS sample household data used in PopulationSim (seed_households.csv)
#'    1.4 ACS PUMS sample person data used in PopulationSim (seed_persons.csv)
#' 2. Open this script in RStudio
#' 3. Press Ctrl + A and then Ctrl + R (or CTrl + ENTER)
#' ##############################################################	

if (!"data.table" %in% installed.packages()) install.packages("data.table", repos='http://cran.us.r-project.org')
library(data.table)
if (!"dplyr" %in% installed.packages()) install.packages("dplyr", repos='http://cran.us.r-project.org')
library(dplyr)
if (!"stringr" %in% installed.packages()) install.packages("stringr", repos='http://cran.us.r-project.org')
library(stringr)
if (!"bit64" %in% installed.packages()) install.packages("bit64", repos='http://cran.us.r-project.org')
library(bit64)

# Set directory
args                <- commandArgs(trailingOnly = TRUE)
Parameters_File     <- args[1]
# Parameters_File <- "E:/Projects/Clients/Nashville/Tasks/Task2_PopSim/Data/parameters.csv"

parameters <- read.csv(Parameters_File, header = TRUE)
basedir <- trimws(paste(parameters$Value[parameters$Key=="POPSIMDIR"]))
xwalkdir <- trimws(paste(parameters$Value[parameters$Key=="XWALK_DIR"]))
xwalk <- read.csv(paste(xwalkdir,"\\geo_crosswalks.csv",sep=""), header = TRUE)

outputDir <- paste0(basedir, "\\output\\DaySimFormat")
dir.create(outputDir, showWarnings = F)

### Helper FUnctions

# Calculates the person type of the synthetic person record
map_pptyp <- function(df)
{
  df$wrkr <- 0
  df$wrkr[df$WKW>0 & df$ESR>=1 & df$ESR<=2] <- 1
  df$wrkr[df$WKW>0 & df$ESR>=4 & df$ESR<=5] <- 1
  df$pwtyp <- 0
  df$pwtyp[df$wrkr==1 & df$WKHP>=32] <- 1
  df$pwtyp[df$wrkr==1 & df$WKHP<32] <- 2
  df$pstyp <- 0
  df$pstyp[df$SCHG>0] <- 1
  df$pptyp <- 0
  df$pptyp[df$pagey<5 & df$pptyp==0] <- 8
  df$pptyp[df$pagey<16 & df$pptyp==0] <- 7
  df$pptyp[df$pwtyp==1 & df$pptyp==0] <- 1
  df$pptyp[df$pstyp==1 & df$SCHG<=14 & df$pptyp==0] <- 6
  df$pptyp[df$pstyp==1 & df$pptyp==0] <- 5
  df$pptyp[df$wrkr==1 & df$pptyp==0] <- 2
  df$pptyp[df$pagey>=65 & df$pptyp==0] <- 3
  df$pptyp[df$pptyp==0] <- 4
  df
}

# add additional per variables
add_pervar <- function(df)
{
  df <- df[,c("hhid","serialno","pnum","maz","pagey","pgend","pwtyp","pstyp","pptyp","wrkr","ADJINC","PINCP")]
  df$pwpcl <- -1
  df$pwtaz <- -1
  df$pwautime <- -1
  df$pwaudist <- -1
  df$pspcl <- -1
  df$pstaz <- -1
  df$psautime <- -1
  df$psaudist <- -1
  df$puwmode <- -1
  df$puwarrp <- -1
  df$puwdepp <- -1
  df$ptpass <- -1
  df$ppaidprk <- -1
  df$pdiary <- -1
  df$pproxy <- -1
  df$psexpfac <- 1
  df
}

# add additional hh variables
add_hhvar <- function(df,styp)
{
  df$hhvehs <- -1
  df$hhftw <- -1
  df$hhptw <- -1
  df$hhret <- -1
  df$hhoad <- -1
  df$hhuni <- -1
  df$hhhsc <- -1
  df$hh515 <- -1
  df$hhcu5 <- -1
  df$hhexpfac <- 1
  df$samptype <- styp
  if(styp==12)
    df <- merge(df,raw_hh_nhts[,c("serialno","hhincome","hrestype","hownrent")],all.x=T)
  if(styp==13)
    df <- merge(df,pervardf[!duplicated(pervardf[,"serialno"]),c("serialno","hhincome")],all.x=T)
  df
}

### Read ACS PUMS sample data
raw_hh_pums <- fread(file.path(basedir,"data/seed_households_GQ.csv"))
raw_per_pums <- fread(file.path(basedir,"data/seed_persons_GQ.csv"))

raw_per_pums$HHINCADJ <- raw_per_pums$ADJINC/1000000*raw_per_pums$PINCP
attach(raw_per_pums)
raw_per_pums$workers[ESR %in% c(1,2,4,5)] <- 1
raw_per_pums$workers[!(ESR %in% c(1,2,4,5))] <- 0
detach(raw_per_pums)

raw_hh_pums <- merge(raw_hh_pums, raw_per_pums %>% select(SERIALNO,HHINCADJ,workers), by ="SERIALNO", all.x=T)
raw_hh_pums$HH <- 1

names(raw_per_pums)[names(raw_per_pums) == "HHINCADJ"] <- "PINCADJ"

names(raw_per_pums)[names(raw_per_pums) == "workers"] <- "wrk"

### Extract original ID from ACS PUMS HH sample data
perm_map <- raw_hh_pums %>% 
  select(hh_id, SERIALNO)

### Read PopSim output HH file
df_hh <- fread(file.path(basedir,"output/GQ/synthetic_households.csv"), header=T)
names(df_hh)[names(df_hh) == "hh_id_pums"] <- "hh_id"
print(colnames(df_hh))
df_hh_m <- merge(df_hh,perm_map,by="hh_id", all.x=T)
names(df_hh_m)[names(df_hh_m) == "SERIALNO"] <- "serialno"
hhperm <- df_hh_m
print("CHECK hhperm cols")
print(names(hhperm))
### Read PopSim output person file
df_per <- fread(file.path(basedir,"output/GQ/synthetic_persons.csv"), header=T)
names(df_per)[names(df_per) == "hh_id_pums"] <- "hh_id"
df_per_m <- merge(df_per,perm_map,by="hh_id",all.x=T)
names(df_per_m)[names(df_per_m) == "SERIALNO"] <- "serialno"
perperm <- df_per_m
### Set daysim input files as output
hhoutfile <- file.path(outputDir,"household_2021_GQ.dat")
peroutfile <- file.path(outputDir,"person_2021_GQ.dat")

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #

### Process synthetic persons (Microzone IDs are coded as 'hhparcel' since DaySim recognizes this variable name)

per_serialnos <- unique(hhperm$serialno)
per_pums <- raw_per_pums[,c("SERIALNO","SPORDER","AGEP","SEX","ESR","WKHP","SCHG","WKW","ADJINC","PINCP"),with=F]
setnames(per_pums,1:4,c("serialno","pnum","pagey","pgend"))
per_pums2 <- perperm[perperm$serialno %in% unique(per_pums$serialno),]
per_pums$personid <- paste(per_pums$serialno,per_pums$pnum,sep = "")
perperm$personid <- paste(perperm$serialno,perperm$per_num,sep = "")
per_pums2 <- merge(perperm,per_pums,by="personid",all.x=T)
per_pums2$pagey <- per_pums2$AGEP
per_pums2 <- map_pptyp(per_pums2)
per_pums2$serialno.y <- NULL
per_pums2$serialno <- per_pums2$serialno.x
per_pums2$serialno.x <- NULL
per_pums2$hhid <- per_pums2$household_id
per_pums2$maz <- per_pums2$MAZ
per_pums2 <- add_pervar(per_pums2)
allper <- per_pums2
names(allper)[which(names(allper)=="hhid")] <- "hhno"
names(allper)[which(names(allper)=="pnum")] <- "pno"
allper <- allper[order(allper$hhno,allper$pno),]
allpervars <- c("hhno","pno","pptyp","pagey","pgend","pwtyp","pwpcl","pwtaz","pwautime","pwaudist","pstyp","pspcl","pstaz",
                "psautime","psaudist","puwmode","puwarrp","puwdepp","ptpass","ppaidprk","pdiary","pproxy","psexpfac")
allper <- allper[,allpervars, with=FALSE]
allper <- allper[!is.na(allper$hhno),]
write.table(allper,peroutfile,row.names=F,quote=F)

### Process synthetic households (Microzone IDs are coded as 'hhparcel' since DaySim recognizes this variable name)

hhperm_2 <- data.table(hhperm)
names(hhperm_2)[names(hhperm_2) == "household_id"] <- "hhid"
names(hhperm_2)[names(hhperm_2) == "MAZ"] <- "hhparcel"
names(hhperm_2)[names(hhperm_2) == "TAZ"] <- "hhtaz"
names(hhperm_2)[names(hhperm_2) == "NP"] <- "hhsize"
hhperm_2$serialno <- as.character(hhperm_2$serialno)
setkey(hhperm_2,serialno)
hh_pums <- raw_hh_pums[,c("SERIALNO","HHINCADJ","BLD","workers"),with=F]
setnames(hh_pums,1,"serialno")
print("CHECK hh_pums COLS:")
print(names(hh_pums))
hh_pums$hhincome <- hh_pums$HHINCADJ
hh_pums$hrestype <- 9
hh_pums$hrestype[hh_pums$BLD %in% 2] <- 1 #Detached single house
hh_pums$hrestype[hh_pums$BLD %in% c(3)] <- 2 #Duplex/triplex/rowhouse
hh_pums$hrestype[hh_pums$BLD %in% c(4:9)] <- 3 #Apartment/condo
hh_pums$hrestype[hh_pums$BLD %in% 1] <- 4 #Mobile home/trailer
hh_pums$hrestype[hh_pums$BLD %in% 10] <- 6 #Other
hh_pums$hrestype[hh_pums$BLD %in% 0] <- -1 #Group Quarter
hh_pums$serialno <- as.character(hh_pums$serialno)
hh_pums <- hh_pums[,c("serialno","hhincome","hrestype","workers"),with=F]
setkey(hh_pums,serialno)
hhperm_3 <- merge(hhperm_2,hh_pums,by="serialno",all.x=T)
print("Check hhperm names")
print(names(hhperm_3))
hhperm_3 <- data.frame(hhperm_3)
hhperm_3$hownrent <- -1
hhperm_3 <- add_hhvar(hhperm_3,11)
allhh <- hhperm_3
print("re check hhperm names")
print(names(hhperm_3))
allhh$hhwkrs <- allhh$workers
names(allhh)[which(names(allhh)=="hhid")] <- "hhno"
names(allhh)[which(names(allhh)=="workers")] <- "hhwkrs"
print(names(allhh))
allhhvars <- c("hhno","hhsize","hhvehs","hhwkrs","hhftw","hhptw","hhret","hhoad","hhuni","hhhsc","hh515","hhcu5",
               "hhincome","hownrent","hrestype","hhparcel","hhexpfac","samptype")
allhh <- allhh[,allhhvars]
allhh$hhincome <- as.integer(allhh$hhincome)
allhh <- allhh[order(allhh$hhno),]
write.table(allhh,hhoutfile,row.names=F,quote=F)

