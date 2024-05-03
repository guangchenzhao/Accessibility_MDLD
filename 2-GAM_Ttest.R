library(data.table)
library(bit64)
library(tidyverse)
library(lme4)
library(mgcv)
library(AICcmodavg)
library(car)

setwd("")

#read device list in Maryland

MD_device = fread("MD_device_list.csv")
MD_device$BGFIPS = as.character(MD_device$h_BGFIPS)

numdevice_bg = count(MD_device,BGFIPS)
colnames(numdevice_bg) = c('BGFIPS','num_device_MDLD')

#Read Residence Area Characteristic data from LEHD Origin-Destination Employment Statistics (LODES)
md_rac = fread("/md_rac_S000_JT00_2019.csv")
md_rac$BGFIPS = substr(as.character(md_rac$h_geocode),1,12)

md_empl = aggregate(md_rac[,2:42] ,by = list(md_rac$BGFIPS), sum)
colnames(md_empl) = c('BGFIPS','num_worker', colnames(md_empl)[3:42])

md_empl[,paste0('pct_', colnames(md_empl)[3:42])] = md_empl[,3:42] / md_empl$num_worker
md_empl$pct_NRM_0102 = md_empl$pct_CNS01 + md_empl$pct_CNS02
md_empl$pct_Trade_0607 = md_empl$pct_CNS06 + md_empl$pct_CNS07
md_empl$pct_Fin_1011 = md_empl$pct_CNS10 + md_empl$pct_CNS11
md_empl$pct_PBS_121314 = md_empl$pct_CNS12 + md_empl$pct_CNS13 + md_empl$pct_CNS14

#Read Workplace Area Characteristic data from LODES
md_wac = fread("md_wac_S000_JT00_2019.csv")
md_wac$BGFIPS = substr(as.character(md_wac$w_geocode),1,12)

md_job = aggregate(md_wac[,2:42] ,by = list(md_wac$BGFIPS), sum)
colnames(md_job) = c('BGFIPS','num_job', colnames(md_job)[3:42])

#read gender & age data from ACS
sex_age = fread('/1_SEX BY AGE.csv')
sex_age = sex_age[,c(7:55,242)]
sex_age[,3:25] = sex_age[,3:25]+sex_age[,27:49]
sex_age = sex_age[,c(1:25,50)]
sex_age$`Geography ID` = substr(sex_age$`Geography ID`,8,10000)
sex_age = sex_age[nchar(sex_age$`Geography ID`) == 12]
sex_age$pop = sex_age$B01001_001
sex_age$pct_male = sex_age$B01001_002 / sex_age$pop
sex_age$pct_under_25 = (sex_age$B01001_003 + sex_age$B01001_004 + sex_age$B01001_005 + sex_age$B01001_006 + 
                          sex_age$B01001_007 + sex_age$B01001_008 + sex_age$B01001_009 + sex_age$B01001_010)/ sex_age$pop
sex_age$pct_under_29 = (sex_age$B01001_003 + sex_age$B01001_004 + sex_age$B01001_005 + sex_age$B01001_006 + 
                        sex_age$B01001_007 + sex_age$B01001_008 + sex_age$B01001_009 + sex_age$B01001_010 + 
                        sex_age$B01001_011)/ sex_age$pop
sex_age$pct_25_44 = (sex_age$B01001_011 + sex_age$B01001_012 + sex_age$B01001_013 + sex_age$B01001_014)/ sex_age$pop
sex_age$pct_30_54 = (sex_age$B01001_012 + sex_age$B01001_013 + sex_age$B01001_014 + 
                       sex_age$B01001_015 + sex_age$B01001_016)/ sex_age$pop
sex_age$pct_45_64 = (sex_age$B01001_015 + sex_age$B01001_016 + sex_age$B01001_017 + sex_age$B01001_018 + 
                       sex_age$B01001_019)/ sex_age$pop
sex_age$pct_55_over = (sex_age$B01001_017 + sex_age$B01001_018 + 
                         sex_age$B01001_019 + sex_age$B01001_020 + sex_age$B01001_021 + sex_age$B01001_022 + sex_age$B01001_023 + 
                         sex_age$B01001_024 + sex_age$B01001_025)/ sex_age$pop
sex_age$pct_65_over = (sex_age$B01001_020 + sex_age$B01001_021 + sex_age$B01001_022 + sex_age$B01001_023 + 
                         sex_age$B01001_024 + sex_age$B01001_025)/ sex_age$pop
sex_age = sex_age[,c(26:35)]
colnames(sex_age) = c('BGFIPS', colnames(sex_age)[2:10])

#read educaton data from ACS
edu = fread('/42_SEX BY AGE BY EDUCATIONAL ATTAINMENT FOR THE POPULATION 18 YEARS AND OVER.csv')
edu$`Geography ID` = substr(edu$`Geography ID`,8,10000)
edu = edu[nchar(edu$`Geography ID`) == 12]
edu = edu[,c(125:149,249)]
edu$pct_BD_25_over = (edu$B15003_022 + edu$B15003_023 + edu$B15003_024 + edu$B15003_025) / edu$B15003_001
edu = edu[,c(26:27)]
colnames(edu) = c('BGFIPS', colnames(edu)[2])

#read median household income data from ACS
medhhinc = fread('/58_HOUSEHOLD INCOME IN THE PAST 12 MONTHS (IN 2019 INFLATION-ADJUSTED DOLLARS).csv')
medhhinc = medhhinc[,c('B19013_001','Geography ID')]
medhhinc$medHHinc = as.integer(medhhinc$B19013_001)
medhhinc$`Geography ID` = substr(medhhinc$`Geography ID`,8,10000)
medhhinc = medhhinc[nchar(medhhinc$`Geography ID`) == 12]
medhhinc$CTFIPS = substr(medhhinc$`Geography ID`,1,5)

#read race data from ACS
race = fread('/3_RACE.csv')
race = race[,c(7:16,223)]
race$`Geography ID` = substr(race$`Geography ID`,8,10000)
race = race[nchar(race$`Geography ID`) == 12]
race$pct_White = race$B02001_002 / race$B02001_001
race$pct_Black = race$B02001_003 / race$B02001_001
race$pct_Asian = race$B02001_005 / race$B02001_001

#read if_Hispanic data from ACS
hisp = fread('/4_HISPANIC OR LATINO ORIGIN BY SPECIFIC ORIGIN.csv')
hisp = hisp[,c(38:39,62)]
hisp$`Geography ID` = substr(hisp$`Geography ID`,8,10000)
hisp = hisp[nchar(hisp$`Geography ID`) == 12]
hisp$pct_Hisp = 1- hisp$B03002_002 / hisp$B03002_001
hisp = hisp[,c(3:4)]
colnames(hisp) = c('BGFIPS', colnames(hisp)[2])

#read percentage of rented household data from ACS                  
rent = fread('/111_HOUSING UNITS.csv')
rent = rent[,c(11:13,250)]
rent$`Geography ID` = substr(rent$`Geography ID`,8,10000)
rent = rent[nchar(rent$`Geography ID`) == 12]
rent$pct_rent = rent$B25003_003 / rent$B25003_001
rent = rent[,c(4:5)]
colnames(rent) = c('BGFIPS', colnames(rent)[2])

#read percentage of urban area data from ACS
urban = fread("/pct_urban_bg.csv")
urban$GEOID = as.character(urban$GEOID)
urban$pct_urban = urban$stateua_83m_pc/100
urban = urban[,c(1,3)]
colnames(urban) = c('BGFIPS', colnames(urban)[2])

#percentage of workers working in different time thresholds in each zone (MDLD-based method)
pct_worker_intime = fread('/pct_worker_intime_all.csv')
pct_worker_intime$BGFIPS = as.character(pct_worker_intime$BGFIPS)
pct_worker_intime = pct_worker_intime[,c(1:2, 9:14)]
colnames(pct_worker_intime) = c('BGFIPS','num_worker_MDLD', colnames(pct_worker_intime)[3:8])

# block group socio-demographic features
MD_CBG_features = fread('/BMC_CBG_features_1115.csv')

MD_CBG_features$BGFIPS = as.character(MD_CBG_features$GEOID10)
BMC_CBG_features = MD_CBG_features[, c('BGFIPS', 'COUNTYFP10', 'TRACTCE10', "ALAND10", "AWATER10", "medHHinc", "pct_White", "pct_Black", "pct_Asian")]

#merge all the above
BMC_CBG_features = merge(BMC_CBG_features, pct_worker_intime, by = 'BGFIPS')

BMC_CBG_features = merge(BMC_CBG_features, numdevice_bg, by = 'BGFIPS', all.x = TRUE)
BMC_CBG_features = merge(BMC_CBG_features, sex_age, by = 'BGFIPS', all.x = TRUE)
BMC_CBG_features = merge(BMC_CBG_features, hisp, by = 'BGFIPS', all.x = TRUE)
BMC_CBG_features = merge(BMC_CBG_features, edu, by = 'BGFIPS', all.x = TRUE)
BMC_CBG_features = merge(BMC_CBG_features, md_empl[,c(1:2, 43:86)], by = 'BGFIPS', all.x = TRUE)
#BMC_CBG_features = merge(BMC_CBG_features, Aland, by = 'BGFIPS', all.x = TRUE)
BMC_CBG_features = merge(BMC_CBG_features, rent, by = 'BGFIPS', all.x = TRUE)
BMC_CBG_features = merge(BMC_CBG_features, urban, by = 'BGFIPS', all.x = TRUE)
BMC_CBG_features = merge(BMC_CBG_features, md_job[,c(1:2)], by = 'BGFIPS', all.x = TRUE)

#percentage of workers without college degree
BMC_CBG_features$pct_worker_nocollege = BMC_CBG_features$pct_CD01 + BMC_CBG_features$pct_CD02
BMC_CBG_features$pct_worker_nocollege = BMC_CBG_features$pct_CD01 + BMC_CBG_features$pct_CD02

BMC_CBG_features$ALAND_mi2 = BMC_CBG_features$ALAND10 /2589988.1103

BMC_CBG_features$pop_dens = BMC_CBG_features$pop / BMC_CBG_features$ALAND_mi2
BMC_CBG_features$worker_dens = BMC_CBG_features$num_worker / BMC_CBG_features$ALAND_mi2
BMC_CBG_features$job_dens = BMC_CBG_features$num_job / BMC_CBG_features$ALAND_mi2
BMC_CBG_features$total_samplerate = BMC_CBG_features$num_device_MDLD/BMC_CBG_features$pop
BMC_CBG_features$worker_samplerate = BMC_CBG_features$num_worker_MDLD/BMC_CBG_features$num_worker
BMC_CBG_features[is.na(BMC_CBG_features$num_job)]$num_job = 0
BMC_CBG_features[is.na(BMC_CBG_features$job_dens)]$job_dens = 0

#number of job opportunities within different time threshold (traditional method)
BMC_CBG_numjobs_15min = fread('/BMC_CBG_numjobs_15min_19.csv')
BMC_CBG_numjobs_20min = fread('/BMC_CBG_numjobs_20min_19.csv')
BMC_CBG_numjobs_30min = fread('/BMC_CBG_numjobs_30min_19.csv')

BMC_CBG_numjobs_15min$BGFIPS = as.character(BMC_CBG_numjobs_15min$BGFIPS)
BMC_CBG_numjobs_20min$BGFIPS = as.character(BMC_CBG_numjobs_20min$BGFIPS)
BMC_CBG_numjobs_30min$BGFIPS = as.character(BMC_CBG_numjobs_30min$BGFIPS)

colnames(BMC_CBG_numjobs_15min) = c('BGFIPS', paste0(colnames(BMC_CBG_numjobs_15min)[2:42], '_15min'))
BMC_CBG_numjobs_15min[,paste0('pct_', colnames(BMC_CBG_numjobs_15min)[3:42])] = BMC_CBG_numjobs_15min[,3:42] / BMC_CBG_numjobs_15min$C000_15min
colnames(BMC_CBG_numjobs_20min) = c('BGFIPS', paste0(colnames(BMC_CBG_numjobs_20min)[2:42], '_20min'))
BMC_CBG_numjobs_20min[,paste0('pct_', colnames(BMC_CBG_numjobs_20min)[3:42])] = BMC_CBG_numjobs_20min[,3:42] / BMC_CBG_numjobs_20min$C000_20min
colnames(BMC_CBG_numjobs_30min) = c('BGFIPS', paste0(colnames(BMC_CBG_numjobs_30min)[2:42], '_30min'))
BMC_CBG_numjobs_30min[,paste0('pct_', colnames(BMC_CBG_numjobs_30min)[3:42])] = BMC_CBG_numjobs_30min[,3:42] / BMC_CBG_numjobs_30min$C000_30min

BMC_CBG_features = merge(BMC_CBG_features, BMC_CBG_numjobs_15min[,c(1:2, 43:82)], by = 'BGFIPS', all.x = TRUE)
BMC_CBG_features = merge(BMC_CBG_features, BMC_CBG_numjobs_20min[,c(1:2, 43:82)], by = 'BGFIPS', all.x = TRUE)
BMC_CBG_features = merge(BMC_CBG_features, BMC_CBG_numjobs_30min[,c(1:2, 43:82)], by = 'BGFIPS', all.x = TRUE)

#rank for comparision
BMC_CBG_features$rank_C000_15min = frank(BMC_CBG_features$C000_15min)
BMC_CBG_features$rank_C000_20min = frank(BMC_CBG_features$C000_20min)
BMC_CBG_features$rank_C000_30min = frank(BMC_CBG_features$C000_30min)

BMC_CBG_features$rank_pct_worker_in15minBG = frank(BMC_CBG_features$pct_worker_in15minBG)
BMC_CBG_features$rank_pct_worker_in20minBG = frank(BMC_CBG_features$pct_worker_in20minBG)
BMC_CBG_features$rank_pct_worker_in30minBG = frank(BMC_CBG_features$pct_worker_in30minBG)

# Spearman_F: larger means more job nearby but less working nearly
BMC_CBG_features$Spearman_F_15min = BMC_CBG_features$rank_C000_15min - BMC_CBG_features$rank_pct_worker_in15minBG
BMC_CBG_features$Spearman_F_20min = BMC_CBG_features$rank_C000_20min - BMC_CBG_features$rank_pct_worker_in20minBG
BMC_CBG_features$Spearman_F_30min = BMC_CBG_features$rank_C000_30min - BMC_CBG_features$rank_pct_worker_in30minBG

BMC_CBG_features$norm_pct_worker_in15minBG = (BMC_CBG_features$pct_worker_in15minBG - min(BMC_CBG_features$pct_worker_in15minBG)) / 
  (max(BMC_CBG_features$pct_worker_in15minBG) - min(BMC_CBG_features$pct_worker_in15minBG))
BMC_CBG_features$norm_pct_worker_in20minBG = (BMC_CBG_features$pct_worker_in20minBG - min(BMC_CBG_features$pct_worker_in20minBG)) / 
  (max(BMC_CBG_features$pct_worker_in20minBG) - min(BMC_CBG_features$pct_worker_in20minBG))
BMC_CBG_features$norm_pct_worker_in30minBG = (BMC_CBG_features$pct_worker_in30minBG - min(BMC_CBG_features$pct_worker_in30minBG)) / 
  (max(BMC_CBG_features$pct_worker_in30minBG) - min(BMC_CBG_features$pct_worker_in30minBG))

BMC_CBG_features$norm_C000_15min = (BMC_CBG_features$C000_15min - min(BMC_CBG_features$C000_15min)) / 
  (max(BMC_CBG_features$C000_15min) - min(BMC_CBG_features$C000_15min))
BMC_CBG_features$norm_C000_20min = (BMC_CBG_features$C000_20min - min(BMC_CBG_features$C000_20min)) / 
  (max(BMC_CBG_features$C000_20min) - min(BMC_CBG_features$C000_20min))
BMC_CBG_features$norm_C000_30min = (BMC_CBG_features$C000_30min - min(BMC_CBG_features$C000_30min)) / 
  (max(BMC_CBG_features$C000_30min) - min(BMC_CBG_features$C000_30min))

# norm: larger means more job nearby but less working nearly
BMC_CBG_features$norm_gap_15min = BMC_CBG_features$norm_C000_15min - BMC_CBG_features$norm_pct_worker_in15minBG
BMC_CBG_features$norm_gap_20min = BMC_CBG_features$norm_C000_20min - BMC_CBG_features$norm_pct_worker_in20minBG
BMC_CBG_features$norm_gap_30min = BMC_CBG_features$norm_C000_30min - BMC_CBG_features$norm_pct_worker_in30minBG

#vif test to detect multicollinearity 
vif_test <- lm(pct_worker_in20minBG ~ 
               + pop_dens + worker_dens + job_dens
               + pct_urban
               + pct_rent
               + pct_CS01
               + pct_CE01 #+ pct_CE02 
               + pct_CE03
               + pct_CA01 + pct_CA03    
               + pct_CR02  + pct_CR04
               + pct_CT02
               + pct_NRM_0102 + pct_CNS03  + pct_CNS04 + pct_CNS05 + pct_Trade_0607  
               + pct_CNS08 + pct_CNS09 + pct_Fin_1011  
               + pct_PBS_121314 
               + pct_CNS15 + pct_CNS16 + pct_CNS17  + pct_CNS18 + pct_CNS19 
               
               ,
               data = BMC_CBG_features[!is.na(medHHinc)& total_samplerate <=1& worker_samplerate <=1& worker_samplerate >=0.006])
vif(vif_test)

#GAM with different time threshold for both traditional and MDLD-based methods

gam_20min_MDLD = glm(
  pct_worker_in20minBG
  ~ + pop_dens + worker_dens + job_dens
  + pct_urban
  + pct_rent
  + pct_CD04  
  + pct_CS01
  + pct_CE01
  + pct_CE03
  + pct_CA01 + pct_CA03    
  + pct_CR02  + pct_CR04
  + pct_CT02
  + pct_NRM_0102 + pct_CNS03  + pct_CNS04 + pct_CNS05 + pct_Trade_0607  
  + pct_CNS08 + pct_CNS09 + pct_Fin_1011  
  + pct_PBS_121314 
  + pct_CNS15 + pct_CNS16 + pct_CNS17  + pct_CNS18 + pct_CNS19 
  + s(COUNTYFP10, k=6, bs='re')+ s(TRACTCE10, bs='re'),
  data = BMC_CBG_features[!is.na(medHHinc)& total_samplerate <=1& worker_samplerate <=1& worker_samplerate >=0.006],
  family = gaussian)
summary(gam_20min_MDLD)

#traditional method also
gam_20min_trad = gam(
  C000_20min
  + pop_dens + worker_dens + job_dens
  + pct_urban
  + pct_rent
  + pct_CD04  
  + pct_CS01
  + pct_CE01 
  + pct_CE03
  + pct_CA01 + pct_CA03    
  + pct_CR02  + pct_CR04
  + pct_CT02
  + pct_NRM_0102 + pct_CNS03  + pct_CNS04 + pct_CNS05 + pct_Trade_0607  
  + pct_CNS08 + pct_CNS09 + pct_Fin_1011  
  + pct_PBS_121314 
  + pct_CNS15 + pct_CNS16 + pct_CNS17  + pct_CNS18 + pct_CNS19 
  + s(COUNTYFP10, bs='re')
  ,
  data = BMC_CBG_features[!is.na(medHHinc)& total_samplerate <=1& worker_samplerate <=1& worker_samplerate >=0.006],
  family = 'gaussian',
  method = 'REML')
summary(gam_20min_trad)

#always see how lm works
lm_20min_MDLD = lm(
  pct_worker_in20minBG
  ~ + pop_dens + worker_dens + job_dens
  + pct_urban
  + pct_rent
  + pct_CD04  
  + pct_CS01
  + pct_CE01 #+ pct_CE02 
  + pct_CE03
  + pct_CA01 + pct_CA03    
  + pct_CR02  + pct_CR04
  + pct_CT02#+ pct_CR03  + pct_CR05 
  + pct_NRM_0102 + pct_CNS03  + pct_CNS04 + pct_CNS05 + pct_Trade_0607  
  + pct_CNS08 + pct_CNS09 + pct_Fin_1011  
  + pct_PBS_121314 
  + pct_CNS15 + pct_CNS16 + pct_CNS17  + pct_CNS18 + pct_CNS19 
  + COUNTYFP10#+ s(TRACTCE10, bs='re')#+ s(total_samplerate, bs='re')
  ,
  data = BMC_CBG_features[!is.na(medHHinc)& total_samplerate <=1& worker_samplerate <=1& worker_samplerate >=0.006])
summary(lm_20min_MDLD)


#shapiro test
st = lapply(BMC_CBG_features_simp[pctl_pct_worker_in20minBG<=0.333 & pctl_C000_20min>0.666], shapiro.test)
lst <- sapply(st, `[`, "p.value")

#T test for overestimated and underestimated areas
BMC_CBG_features_ttest = as.data.table(t(t(BMC_CBG_features_simp[,c(4:83)]) - colMeans(BMC_CBG_features_simp[,c(4:83)], na.rm = TRUE)))
BMC_CBG_features_ttest = cbind(BMC_CBG_features_ttest, BMC_CBG_features_simp[,c(106:111)]) 
BMC_CBG_features_ttest[]

ttest_1 = sapply(BMC_CBG_features_ttest[pctl_C000_20min-pctl_pct_worker_in20minBG>=1/3],t.test)
ttest_2 = sapply(BMC_CBG_features_ttest[pctl_C000_20min-pctl_pct_worker_in20minBG<=-1/3],t.test)

lttest_1 = as.data.table(ttest_1)
lttest_1 = lttest_1[c(3,5),]
coif_lttest_1 = as.vector(unlist(lttest_1[2]))

lttest_2 = as.data.table(ttest_2)
lttest_2 = lttest_2[c(3,5),]
coif_lttest_2 = as.vector(unlist(lttest_2[2]))
