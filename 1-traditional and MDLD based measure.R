library(data.table)

# travel time between statewide modeling zone (SMZ)
timMatrix <- fread("/HWY_NT_4_da1time.csv")
traveltime = as.data.table(c(as.matrix(timMatrix[,-'da1time'])))
colnames(traveltime) = c('traveltime')

#separate and run in parallel for too many zones
# Function of finding travel time between SMZs in parallel
find_SMZ_pairs=function(core){
  b=timMatrix$da1time[(round(core/100*nrow(timMatrix))+1):(round((core+1)/100*nrow(timMatrix)))]
  SMZ_pairs = data.table()
  for (i in b){
    #if(i %% 10==0){print(i)}
    for (j in timMatrix$da1time){
      SMZ_pairs = rbind(SMZ_pairs, as.data.table(t(c(j,i))))
    }
  }
  colnames(SMZ_pairs) = c('SMZ_1', 'SMZ_2')
  fwrite(SMZ_pairs,paste0("/SMZ_pairs_",core,".csv"))
}

start=Sys.time()
#Run the function in Parallel
for (loop_id in 0:2){
  #print(loop_id)
  cl = makeCluster(33)
  registerDoSNOW(cl)
  getDoParWorkers()
  foreach(core=((loop_id*33+1):((loop_id+1)*33)),.packages = (.packages()))%dopar%{
    find_SMZ_pairs(core)
  }
  stopCluster(cl)
  rm(cl)
  closeAllConnections()
  gc()
}
end=Sys.time()

mypath ="/SMZ_pairs"
zipF <- list.files(path = mypath,pattern = "SMZ_pairs_", full.names = TRUE)
SMZ_pairs = data.table()
for (i in 1:length(zipF)){
  print(i)
  data=fread(zipF[i])
  SMZ_pairs=rbind(SMZ_pairs,data)
}
SMZ_pairs = SMZ_pairs[order(SMZ_2, SMZ_1),]
SMZ_pairs$traveltime = traveltime
fwrite(SMZ_pairs,"/SMZ_pairs_Ttime.csv")

################################################### CBG_SMZ join
SMZ_pairs_Ttime = fread("/SMZ_pairs_Ttime.csv")
BMC_CBG_SMZ = fread("/BMC_CBG_latlon_SMZ.csv")[, c('GEOID10', 'SMZ')]
BMC_CBG_SMZ_pairs_Ttime = merge(BMC_CBG_SMZ, SMZ_pairs_Ttime, by.x = 'SMZ', by.y = 'SMZ_1', all.x = TRUE)
BMC_CBG_SMZ_pairs_Ttime_60min = BMC_CBG_SMZ_pairs_Ttime[traveltime<=60]
colnames(BMC_CBG_SMZ_pairs_Ttime_60min) = c('SMZ_1', 'BGFIPS_1', 'SMZ_2', 'traveltime')

CBG_SMZ = fread("/CBG_latlon_SMZ.csv")[, c('GEOID10', 'SMZ')]
BMC_CBG_SMZ_pairs_Ttime_60min = merge(BMC_CBG_SMZ_pairs_Ttime_60min, CBG_SMZ, by.x = 'SMZ_2', by.y = 'SMZ', allow.cartesian=TRUE)
colnames(BMC_CBG_SMZ_pairs_Ttime_60min) = c('SMZ_2','SMZ_1', 'BGFIPS_1', 'traveltime', 'BGFIPS_2')
BMC_CBG_SMZ_pairs_Ttime_60min = BMC_CBG_SMZ_pairs_Ttime_60min[, c('BGFIPS_1', 'BGFIPS_2', 'SMZ_1','SMZ_2', 'traveltime')]
BMC_CBG_SMZ_pairs_Ttime_60min$BGFIPS_1 = as.character(BMC_CBG_SMZ_pairs_Ttime_60min$BGFIPS_1)
BMC_CBG_SMZ_pairs_Ttime_60min$BGFIPS_2 = as.character(BMC_CBG_SMZ_pairs_Ttime_60min$BGFIPS_2)

############################# num job by category from LODES wac
wacfilepath = "/LODES7"
wacfilelist <- list.files(path = wacfilepath,pattern = "_wac_S000_JT00_2019", full.names = TRUE)
wacfiles = data.table()
for (i in 1:length(wacfilelist)){
  data=fread(wacfilelist[i])
  wacfiles=rbind(wacfiles,data)
}
wacfiles$w_BGFIPS =  substr(wacfiles$w_geocode, 1, 12)

worker_bycatbyBG_LODES = aggregate(wacfiles[,c(2:42)], list(w_BGFIPS = wacfiles$w_BGFIPS), sum)
worker_bycatbyBG_LODES[, c(paste0("pct_",colnames(worker_bycatbyBG_LODES)[3:42]))] = worker_bycatbyBG_LODES[, c(3:42)] / worker_bycatbyBG_LODES$C000
fwrite(worker_bycatbyBG_LODES,"/worker_bycatbyBG_LODES_19.csv")

#traditional method: number of jobs within a certain time threshold
worker_bycatbyBG_LODES = fread("/worker_bycatbyBG_LODES_19.csv")
worker_bycatbyBG_LODES$w_BGFIPS = as.character(worker_bycatbyBG_LODES$w_BGFIPS)

BMC_CBG_SMZ_pairs_Ttime_60min_numjobs = merge(BMC_CBG_SMZ_pairs_Ttime_60min, worker_bycatbyBG_LODES, by.x = 'BGFIPS_2', by.y = 'w_BGFIPS', all.x = TRUE)
BMC_CBG_SMZ_pairs_Ttime_60min_numjobs = BMC_CBG_SMZ_pairs_Ttime_60min_numjobs[,c(2, 1, 3:86)]

BMC_CBG_SMZ_pairs_Ttime_60min_numjobs[is.na(BMC_CBG_SMZ_pairs_Ttime_60min_numjobs)] = 0
BMC_CBG_numjobs_60min=aggregate(BMC_CBG_SMZ_pairs_Ttime_60min_numjobs[,c(6:46)], 
                                list(BGFIPS = BMC_CBG_SMZ_pairs_Ttime_60min_numjobs$BGFIPS_1), sum)
BMC_CBG_numjobs_45min=aggregate(BMC_CBG_SMZ_pairs_Ttime_60min_numjobs[traveltime<=45][,c(6:46)], 
                                list(BGFIPS = BMC_CBG_SMZ_pairs_Ttime_60min_numjobs[traveltime<=45]$BGFIPS_1), sum)
BMC_CBG_numjobs_30min=aggregate(BMC_CBG_SMZ_pairs_Ttime_60min_numjobs[traveltime<=30][,c(6:46)], 
                                list(BGFIPS = BMC_CBG_SMZ_pairs_Ttime_60min_numjobs[traveltime<=30]$BGFIPS_1), sum)
BMC_CBG_numjobs_20min=aggregate(BMC_CBG_SMZ_pairs_Ttime_60min_numjobs[traveltime<=20][,c(6:46)], 
                                list(BGFIPS = BMC_CBG_SMZ_pairs_Ttime_60min_numjobs[traveltime<=20]$BGFIPS_1), sum)
BMC_CBG_numjobs_15min=aggregate(BMC_CBG_SMZ_pairs_Ttime_60min_numjobs[traveltime<=15][,c(6:46)], 
                                list(BGFIPS = BMC_CBG_SMZ_pairs_Ttime_60min_numjobs[traveltime<=15]$BGFIPS_1), sum)

#MDLD-based method: percentage of works who actually work in those zones within a certain time threshold
#worker and there H&W location from MDLD
MD_worker_list_02_01add = fread("/MD_worker_list_02_01add.csv")
MD_worker_list_02_01add$h_BGFIPS = as.character(MD_worker_list_02_01add$h_BGFIPS)
MD_worker_list_02_01add$h_CTFIPS =  substr(MD_worker_list_02_01add$h_BGFIPS, 1, 5)
BMC_worker_list = MD_worker_list_02_01add[h_CTFIPS %in% c('24003', '24005', '24013', '24025', '24027', '24035', '24510')]

BMC_BGlist = unique(BMC_worker_list$h_BGFIPS)
pct_worker_intime=data.table()
pct_worker_intime$BGFIPS = BMC_BGlist
for(i in 1:nrow(pct_worker_intime)){
  if(i %% 100==0){print(i)}
  h_BGFIPS_i= pct_worker_intime$BGFIPS[i]
  worker_BG = BMC_worker_list[h_BGFIPS == h_BGFIPS_i]
  pct_worker_intime$num_worker[i] = nrow(worker_BG)
  pct_worker_intime$num_worker_in10minBG[i] = nrow(worker_BG[w_BGFIPS %in% BMC_CBG_SMZ_pairs_Ttime_60min[(BGFIPS_1 == h_BGFIPS_i) & (traveltime <= 10)]$BGFIPS_2])
  pct_worker_intime$num_worker_in15minBG[i] = nrow(worker_BG[w_BGFIPS %in% BMC_CBG_SMZ_pairs_Ttime_60min[(BGFIPS_1 == h_BGFIPS_i) & (traveltime <= 15)]$BGFIPS_2])
  pct_worker_intime$num_worker_in20minBG[i] = nrow(worker_BG[w_BGFIPS %in% BMC_CBG_SMZ_pairs_Ttime_60min[(BGFIPS_1 == h_BGFIPS_i) & (traveltime <= 20)]$BGFIPS_2])
  pct_worker_intime$num_worker_in30minBG[i] = nrow(worker_BG[w_BGFIPS %in% BMC_CBG_SMZ_pairs_Ttime_60min[(BGFIPS_1 == h_BGFIPS_i) & (traveltime <= 30)]$BGFIPS_2])
  pct_worker_intime$num_worker_in45minBG[i] = nrow(worker_BG[w_BGFIPS %in% BMC_CBG_SMZ_pairs_Ttime_60min[(BGFIPS_1 == h_BGFIPS_i) & (traveltime <= 45)]$BGFIPS_2])
  pct_worker_intime$num_worker_in60minBG[i] = nrow(worker_BG[w_BGFIPS %in% BMC_CBG_SMZ_pairs_Ttime_60min[(BGFIPS_1 == h_BGFIPS_i) & (traveltime <= 60)]$BGFIPS_2])
}

pct_worker_intime[, c("pct_worker_in10minBG","pct_worker_in15minBG","pct_worker_in20minBG",
                      "pct_worker_in30minBG","pct_worker_in45minBG","pct_worker_in60minBG")] = pct_worker_intime[, c(3:8)] / pct_worker_intime$num_worker

fwrite(pct_worker_intime,"/pct_worker_intime_all.csv")
  
  