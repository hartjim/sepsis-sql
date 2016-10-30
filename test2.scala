/** hard-coding parameters since query is from a stored procedure
@StartDate = "2016-01-01 00:00:00.0"
@EndDate = "2016-01-31 23:59:00.0"
@Institution = "MHR"
@ServicingFacility = "SMM"
  */


import java.sql.Timestamp


import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, LongType}


def convertToDateTime: Long => Timestamp = new Timestamp(_)

def fixDateTimeColumn(columnName: String, df: DataFrame) = findColumn(df, columnName, LongType)
  .map(udf(convertToDateTime).apply(_))
  .getOrElse(df.col(columnName))


def findColumn(df: DataFrame, colName: String, colType: DataType) = {
  val existingFieldStruct = df.schema.find(f => {
    f.name == colName && f.dataType == colType
  })
  existingFieldStruct.map(structField => df.col(structField.name))
}

/** table definitions */

//val hdfs_path = "hdfs://10.0.100.252:8020/user/hdfs/stjoe/amalga/sqooped/2016/09/01"
val hdfs_path = "file:///Users/jimmarczyk/01/"

val VISITPT_ALL_DIS = sqlContext.read.parquet(s"$hdfs_path/azViews/VISITPT_ALL_DIS/*.parquet")
VISITPT_ALL_DIS
  .withColumn("AdmitDateTime_f", fixDateTimeColumn("AdmitDateTime", VISITPT_ALL_DIS))
  .withColumn("DischargeDateTime_f", fixDateTimeColumn("DischargeDateTime", VISITPT_ALL_DIS))
  .withColumn("ERAdmitDtTm_f", fixDateTimeColumn("ERAdmitDtTm", VISITPT_ALL_DIS))
  .withColumn("LastIPVisitDtTm_f", fixDateTimeColumn("LastIPVisitDtTm", VISITPT_ALL_DIS))
  .drop("AdmitDateTime")
  .drop("DischargeDateTime")
  .drop("ERAdmitDtTm")
  .drop("LastIPVisitDtTm")
  .withColumnRenamed("AdmitDateTime_f", "AdmitDateTime")
  .withColumnRenamed("DischargeDateTime_f", "DischargeDateTime")
  .withColumnRenamed("ERAdmitDtTm_f", "ERAdmitDtTm")
  .withColumnRenamed("LastIPVisitDtTm_f", "LastIPVisitDtTm")
  .registerTempTable("VISITPT_ALL_DIS")

val QryIntVisitPT_All_Activity = sqlContext.read.parquet(s"$hdfs_path/azViews/QryIntVisitPT_All_Activity/*.parquet")
QryIntVisitPT_All_Activity
  .withColumn("ActivityDate_Time_f", fixDateTimeColumn("ActivityDate_Time", QryIntVisitPT_All_Activity))
  .drop("ActivityDate_Time")
  .withColumnRenamed("ActivityDate_Time_f", "ActivityDate_Time")
  .registerTempTable("QryIntVisitPT_All_Activity")

val OrderVisitPT_All_Activity = sqlContext.read.parquet(s"$hdfs_path/azViews/OrderVisitPT_All_Activity/*.parquet")
OrderVisitPT_All_Activity
  .withColumn("OrderDateTime_f", fixDateTimeColumn("OrderDateTime", OrderVisitPT_All_Activity))
  .drop("OrderDateTime")
  .withColumnRenamed("OrderDateTime_f", "OrderDateTime")
  .registerTempTable("OrderVisitPT_All_Activity")

val Dictionary_OrderSetDetails_Lookup = sqlContext.read.parquet(s"$hdfs_path/azViews/Dictionary_OrderSetDetails_Lookup/*.parquet")
Dictionary_OrderSetDetails_Lookup.registerTempTable("Dictionary_OrderSetDetails_Lookup")

val MCPathBBPt_All_ADM = sqlContext.read.parquet(s"$hdfs_path/azViews/MCPathBBPt_All_ADM/*.parquet")
MCPathBBPt_All_ADM
  .withColumn("Collected_Date_Time_f", fixDateTimeColumn("Collected_Date_Time", MCPathBBPt_All_ADM))
  .drop("Collected_Date_Time")
  .withColumnRenamed("Collected_Date_Time_f", "Collected_Date_Time")
  .registerTempTable("MCPathBBPt_All_ADM")

val LABVISITPT_ALL_DIS = sqlContext.read.parquet(s"$hdfs_path/azViews/LABVISITPT_ALL_DIS/*.parquet")
LABVISITPT_ALL_DIS
  .withColumn("CollectedDateTime_f", fixDateTimeColumn("CollectedDateTime", LABVISITPT_ALL_DIS))
  .withColumn("DischargeDateTime_f", fixDateTimeColumn("DischargeDateTime", LABVISITPT_ALL_DIS))
  .drop("CollectedDateTime")
  .drop("DischargeDateTime")
  .withColumnRenamed("CollectedDateTime_f", "CollectedDateTime")
  .withColumnRenamed("DischargeDateTime_f", "DischargeDateTime")
  .registerTempTable("LABVISITPT_ALL_DIS")

val ABS_DG1601 = sqlContext.read.parquet(s"$hdfs_path/azABS/ABS_DG1601/*.parquet")
ABS_DG1601.registerTempTable("ABS_DG1601")

val Encounter_Patient_MDR = sqlContext.read.parquet(s"$hdfs_path/sjhsEncounterData/Encounter_Patient_MDR/*.parquet")
Encounter_Patient_MDR.registerTempTable("Encounter_Patient_MDR")

val AdmVitalSigns = sqlContext.read.parquet(s"$hdfs_path/sjhsADM/AdmVitalSigns/*.parquet")
AdmVitalSigns
  .withColumn("ArrivalDateTime_f", fixDateTimeColumn("ArrivalDateTime", AdmVitalSigns))
  .drop("ArrivalDateTime")
  .withColumnRenamed("ArrivalDateTime_f", "ArrivalDateTime")
  .registerTempTable("AdmVitalSigns")

val EdmPatientStatusEventHistory = sqlContext.read.parquet(s"$hdfs_path/sjhsEDM/EdmPatientStatusEventHistory/*.parquet")
EdmPatientStatusEventHistory
  .withColumn("StartDateTime_f", fixDateTimeColumn("StartDateTime", EdmPatientStatusEventHistory))
  .drop("StartDateTime")
  .withColumnRenamed("StartDateTime_f", "StartDateTime")
  .registerTempTable("EdmPatientStatusEventHistory")

val ADT_Lookup_Provider = sqlContext.read.parquet(s"$hdfs_path/azADT/ADT_Lookup_Provider/*.parquet")
ADT_Lookup_Provider.registerTempTable("ADT_Lookup_Provider")

val PhaRxAdministrations = sqlContext.read.parquet(s"$hdfs_path/sjhsPHA/PhaRxAdministrations/*.parquet")
PhaRxAdministrations.registerTempTable("PhaRxAdministrations")

val AbstractData = sqlContext.read.parquet(s"$hdfs_path/sjhsRCS/AbstractData/*.parquet")
AbstractData.registerTempTable("AbstractData")

val PhaRx = sqlContext.read.parquet(s"$hdfs_path/sjhsOE/PhaRx/*.parquet")
PhaRx.registerTempTable("PhaRx")

val PhaRxMedications = sqlContext.read.parquet(s"$hdfs_path/sjhsPHA/PhaRxMedications/*.parquet")
PhaRxMedications.registerTempTable("PhaRxMedications")

val DPhaDrugData = sqlContext.read.parquet(s"$hdfs_path/sjhsOE/DPhaDrugData/*.parquet")
DPhaDrugData.registerTempTable("DPhaDrugData")


val querytmp0 =
              """
  SELECT DISTINCT DIS.EID
    ,DIS.Institution
    ,DIS.ServicingFacility
    ,DIS.MRN
    ,DIS.Account
    ,DIS.Name AS Patient_Name
    ,DIS.AdmitDateTime
    ,DIS.AdmitDate
    ,DIS.AdmitTime
    ,DIS.Age
    ,DIS.Sex AS Gender
    ,DIS.DxCodes
    ,DIS.DRG
    ,DIS.DischargeDateTime
    ,DIS.AttendingMDName AS Attending_Physician
    ,EN.ERPhys
    ,CONCAT(ERMD.FirstName, ' ', ERMD.LastName) AS ED_Physician
    ,DIS.AdmitComplaint AS Reason_for_Visit
    ,DIS.DischargeDisposition
    ,DIS.ERAdmitDtTm AS ED_Received_Date_Time
    ,AVS.ArrivalDateTime AS ED_Arrival_Date_Time
    ,DF.POA
    ,CASE -- how to test?
      WHEN DIS.DischargeDisposition = 'EXP'
        THEN 'Yes'
      ELSE 'No'
      END AS Expired
    ,EN.MortalityRisk AS AprDRGRiskMortality
    ,EN.Severity1 AS AprDRGSeverity
    ,EDT.StartDateTime AS TriageDateTime
    ,DATEDIFF(EDT.StartDateTime, AVS.ArrivalDateTime) AS Wait_Time
    ,DIS.LOS
    ,DIS.Total_Cost
    ,DIS.DaysSinceLastIPVisit
    ,DIS.LastIPVisitDtTm
    ,DIS.PreviousDRG
  FROM VISITPT_ALL_DIS DIS
  INNER JOIN ABS_DG1601 DF ON DIS.EID = DF.EID
  INNER JOIN Encounter_Patient_MDR EN ON DIS.EID = EN.EID
  LEFT JOIN AdmVitalSigns AVS ON EN.EID = AVS.EID
  LEFT JOIN EdmPatientStatusEventHistory EDT ON EN.EID = EDT.EID
    AND EDT.EventID = 'TRIAGE'
  LEFT JOIN ADT_Lookup_Provider ERMD On EN.ERPhys = ERMD.Mnemonic
    AND EN.Institution = ERMD.Ministry
  WHERE DIS.PatientType = 'IN'
    AND DIS.AccountStatus = 'DIS'
    AND DIS.DischargeDateTime BETWEEN '2016-01-01 00:00:00.0'
    AND '2016-01-31 23:59:00.0'
    AND DIS.Institution IN ("MHR")
    AND DIS.ServicingFacility IN ("SMM")
    AND DF.DxCode IN (
       '785.52'
      ,'995.91'
      ,'995.92'
      ,'R65.21'
      ,'A41.9'
      ,'R65.20'
      ,'A021'
      ,'A227'
      ,'A267'
      ,'A327'
      ,'A400'
      ,'A401'
      ,'A403'
      ,'A408'
      ,'A409'
      ,'A4101'
      ,'A4102'
      ,'A411'
      ,'A412'
      ,'A413'
      ,'A414'
      ,'A4150'
      ,'A4151'
      ,'A4152'
      ,'A4153'
      ,'A4159'
      ,'A4181'
      ,'A4189'
      ,'A419'
      ,'A427'
      ,'A5486'
      ,'B377'
      ,'R6520'
      ,'R6521')
              """
val MN = sqlContext.sql(querytmp0)
MN.registerTempTable("MN")

val querytmp1 =
            """
  SELECT EID
    ,MIN(ActivityDate_Time) AS SepsisDoneDate
    ,'Y' AS SepsisScreenDone
  FROM QryIntVisitPT_All_Activity
  WHERE IntervenID IN (
     '0250530'
    ,'0250540'
    ,'0300590'
    ,'0301105'
    ,'0300895'
    ,'0301100'
    ,'0150000'
    ,'0300330'
    ,'0150005')
  AND Ctr = '1'
  AND Query IN (
     'NSEPINCR'
    ,'NSEPSECA'
    ,'NSEPSIRS01'
    ,'NSEPSECB'
    ,'NSEPPOS'
    ,'NSEPPHY'
    ,'NSEPOS01'
    ,'NSEPOD01'
    ,'NSEPSECC'
    ,'NSEPSEV'
    ,'NSEPNIN'
    ,'NPEDSETT'
    ,'NPEDSETI'
    ,'NPEDSETH'
    ,'NPEDSETR'
    ,'NPEDSETC'
    ,'NPEDSETN'
    ,'ESPINFP'
    ,'ESPFVR'
    ,'ESPNEURO'
    ,'NSEPNPOS'
    ,'ESPPOSTV'
    ,'NSEPOD'
    ,'NSEPSIRS')
  GROUP BY EID
              """
val SS = sqlContext.sql(querytmp1)
SS.registerTempTable("SS")

val querytmp2 =
              """
  SELECT EID
    ,MIN(ActivityDate_Time) AS FirstSepsisDoneDate
  FROM QryIntVisitPT_All_Activity AS DIS1
  WHERE DIS1.Assessment IN (
     'NU.EESEPSISSC03'
    ,'ED.EEADTRIAGE14'
    ,'ED.TSTSEPSIS'
    ,'ED.TSTSEPSIS1'
    ,'NU.EESEPSISSC02'
    ,'NU.EESEPSISSCRN'
    ,'NUR.RRTSEPSIS'
    ,'NUR.RRTSEPSIS1'
    ,'NUR.SEPSIS'
    ,'NUR.SEPSIS2'
    ,'NU.EESEPSISSC01'
    ,'NUR.SEPSIS1'
    )
    AND DIS1.Query = 'NSEPPOS'
    AND DIS1.QueryText2 LIKE '%Positive%'
  GROUP BY EID
             """
val FSS = sqlContext.sql(querytmp2)
FSS.registerTempTable("FSS")

val querytmp3 =
            """
  SELECT ORA.EID
    ,ORA.OrderDateTime
    ,ORD.Description AS OrderDescription
  FROM OrderVisitPT_All_Activity AS ORA
  LEFT JOIN Dictionary_OrderSetDetails_Lookup AS ORD ON ORA.SourceID = ORD.SourceID
    AND ORA.OrderedProcedure = ORD.Code
  WHERE ORA.OrderedProcedure IN (
    'EEEDPSEP1',
    'NUCCSEPS1',
    'NUPEDSEPS1',
    'NUSDSEPS1',
    'NUQEDNSP1',
    'NUREDNSP1',
    'NUSEDNSP1',
    'NUKEDPSM1',
    'NUPEDPSM1',
    'NUREDPSM1',
    'NUSEDPSM1',
    'NUQEDPSM1',
    'NUMEDNSS1',
    'EEEDNFVSP1',
    'SUMEDSEP1',
    'SUEDSEPS1',
    'SUEDPSEP1',
    'SUCNVEGT2',
    'SUCNVSEP3',
    'SUMEDSSA3',
    'SUCCSEP1',
    'SUEDPSEP1',
    'SUNBJSEP1',
    'EEEDPSEP1',
    'TUEDSEPS1',
    'TUPEDSSE2',
    'EEDSEPS1',
    'EBCNVPSEP1',
    'EECNVPSEP1',
    'EEPEDSEP1',
    'EIEDPSEP1',
    'NQEDSEPPD2',
    'NUKEDNSP1',
    'NUKEDSEPS2',
    'NUPEDNSP1',
    'NUPEDSEPS2',
    'NUQEDSEPS3',
    'NUREDSEPS2',
    'NUSEDNSP2',
    'SIEDPSEP1',
    'SIEDSEPS1',
    'STEDPSEP1',
    'STEDSEPS1',
    'SUCNVPSEP1',
    'SUMETSEP1',
    'SXEDFSAY1',
    'SXEDPSEP1',
    'SXEDSEPS1',
    'TBMEDSEP1',
    'TIEDSEPS1',
    'TUMEDSEP1',
    'TUPEDSEP1',
    'TUPEDSSE2',
    'TPPEDSEP1',
    'STEDSEPS2',
    'SIEDSEPS2',
    'SXEDSEPS2',
    'SUCCSEP2'
    )
                 """
val OE = sqlContext.sql(querytmp3)
OE.registerTempTable("OE")

val querytmp4 =
                """
SELECT EID
  ,MIN(Collected_Date_Time) AS First_BC
FROM MCPathBBPt_All_ADM AS BC
WHERE Observation IN (
   'Blood Culture PCR'
  ,'Blood Culture'
  ,'Blood Culture GS'
  ,'Blood Culture Results'
  ,'Blood Culture Workup'
  ,'Fungus Blood Culture Results'
  ,'Fungus Blood Culture Workup'
  ,'Gram Stain/Blood Culture Media'
  )
AND Result NOT IN (
  'Test not performed '
  ,'                         Test not performed '
  )
  GROUP BY EID
                """
val FB = sqlContext.sql(querytmp4)
FB.registerTempTable("FB")

val querytmp5 =
                """
  SELECT LAB.EID
    ,MIN(LAB.CollectedDateTime) AS First_WBC
    ,LAB.observationvalue AS WBCResult
  FROM LABVISITPT_ALL_DIS AS LAB
  WHERE LAB.observation = 'White Blood Cell Count'
    AND LAB.Institution IN ("MHR")
  GROUP BY LAB.EID
    ,LAB.observationvalue
                """
val FW = sqlContext.sql(querytmp5)
FW.registerTempTable("FW")

val querytmp6 =
                """
  SELECT EID AS FEID
    ,universalserviceid
    ,MIN(CollectedDateTime) AS LactateDate
    ,observationvalue AS LactateResult
  FROM LABVISITPT_ALL_DIS
  WHERE Institution IN ('MHR')
    AND DischargeDateTime BETWEEN '2016-01-01 00:00:00.0'
    AND '2016-01-31 23:59:00.0'
    AND OriginalObservationId IN (
       'LACBG'
      ,'LACTATEPC'
      ,'LACTBFL'
      ,'LACTCCSF'
      ,'LACTIC'
      ,'LAPC'
      ,'LAPCA'
      ,'LACTIC'
      ,'LAPC'
      ,'LAPCA'
      ,'LACBG'
      ,'LACTCCSF'
      ,'LACTIC'
      ,'LAPC'
      ,'LACBGS'
      )
  GROUP BY EID
    ,universalserviceid
    ,observationvalue
               """
val LC = sqlContext.sql(querytmp6)
LC.registerTempTable("LC")

val querytmp7 =
                """
  SELECT MN.*
    ,FB.First_BC
    ,FW.First_WBC
    ,FW.WBCResult
    ,RANK() OVER (PARTITION BY FW.EID ORDER BY FW.First_WBC) AS RankL
    ,FSS.FirstSepsisDoneDate
    ,SS.SepsisScreenDone
    ,SS.SepsisDoneDate
    ,OE.OrderDateTime
    ,OE.OrderDescription
  FROM MN
  LEFT JOIN SS ON MN.EID = SS.EID
  LEFT JOIN FSS ON MN.EID = FSS.EID
  LEFT JOIN OE ON MN.EID = OE.EID
  LEFT JOIN FB ON MN.EID = FB.EID
  LEFT JOIN FW ON MN.EID = FW.EID
                """
val FINAL = sqlContext.sql(querytmp7)
FINAL.registerTempTable("FINAL")

val querytmp8 =
  """
  SELECT *
  FROM FINAL
WHERE RankL = 1
  """
val TEMP1 = sqlContext.sql(querytmp8)
TEMP1.registerTempTable("TEMP1")


val querytmp9 =
  """
  SELECT TEMP1.EID AS PEID
  FROM TEMP1
  INNER JOIN PhaRxAdministrations PRA ON TEMP1.EID = PRA.EID
  INNER JOIN AbstractData AD ON PRA.SourceID = AD.SourceID AND PRA.VisitID = AD.VisitID
  LEFT JOIN PhaRx PR ON PRA.SourceID = PR.SourceID AND PRA.PrescriptionID = PR.PrescriptionID AND PRA.VisitID = PR.VisitID
  LEFT JOIN PhaRxMedications PRM ON PR.SourceID = PRM.SourceID AND PR.PrescriptionID = PRM.PrescriptionID
  """
val MEDMAR = sqlContext.sql(querytmp9)
MEDMAR.registerTempTable("MEDMAR")



val querytmp9 =
              """
  SELECT  PRA.EID as PEID
  ,AD.AccountNumber AS MEDAccount
  ,MIN(PRA.AdministrationDateTime) AS 'First Abx Admin Date'
  ,Rank () OVER (Partition by PRA.EID ORDER by PRA.AdministrationDateTime) AS MEDRANK
  ,DPD.TypeName AS Drug_Type_Description
  ,DPD.Name AS DrugName
  FROM TEMP1
  INNER join  PhaRxAdministrations PRA
    On 	TEMP1.EID = PRA.EID
  INNER JOIN AbstractData AD ON PRA.SourceID = AD.SourceID
    AND PRA.VisitID = AD.VisitID
  LEFT JOIN PhaRx PR ON PRA.SourceID = PR.SourceID
    AND PRA.PrescriptionID = PR.PrescriptionID
    AND PRA.VisitID = PR.VisitID
  LEFT JOIN PhaRxMedications PRM ON PR.SourceID = PRM.SourceID
    AND PR.PrescriptionID = PRM.PrescriptionID
  INNER JOIN DPhaDrugData DPD ON PRM.SourceID = DPD.SourceID
    AND PRM.DrugID = DPD.DrugID COLLATE SQL_Latin1_General_CP1_CS_AS
//  WHERE DPD.TypeID IN (
//     '08:12.02'
//    ,'08:12.28'
//    ,'08:14.92'
//    ,'08:16.92'
//    ,'08:30.92'
//    ,'08:16.04'
//    ,'08:18.92'
//    ,'08:14.08'
//    ,'08:12.06'
//    ,'08:12.08'
//    ,'08:14.16'
//    ,'08:12.12'
//    ,'08:12.07'
//    ,'08:18.24'
//    ,'08:18.32'
//    ,'08:12.16'
//    ,'08:14.28'
//    ,'08:12.18'
//    ,'08:12.20'
//    ,'08:12.24'
//    )
//  GROUP BY PRA.EID
//    ,AD.AccountNumber
//    ,PRA.AdministrationDateTime
//    ,DPD.Name
//    ,DPD.TypeName
//    )
              """
val MEDMAR = sqlContext.sql(querytmp9)
MEDMAR.registerTempTable("MEDMAR")


val querytmp10 =
  """
  SELECT *

  FROM MEDMAR

  WHERE MEDMAR.MEDRANK=1

  """
val #MED1 = sqlContext.sql(querytmp10)

#MED1.registerTempTable("#MED1")





val querytmp12 =
  """

  SELECT  *,
    CASE

      WHEN (#Med1.[First Abx Admin Date]) <= dateadd(hh, 3, (

        CASE

          WHEN TEMP1.[ED Received Date Time] IS NULL

            THEN TEMP1.[ED Received Date Time]
          ELSE TEMP1.[ED Received Date Time]

END

      ))

      THEN 'Y'

    ELSE 'N'
    END

  AS ArrivalAbx

FROM TEMP1

  INNER JOIN #MED1 On TEMP1.EID=#MED1.PEID

  """

val #MEDF = sqlContext.sql(querytmp12)

#MEDF.registerTempTable("#MEDF")





val querytmp13 =
  """

  SELECT *

    ,RANK() OVER (

      ,PARTITION BY FEID ORDER BY [LactateDate]

      ) AS Rn

  FROM #MEDF

  LEFT JOIN Lc ON #MEDF.EID = Lc.FEID

  """
val LAC = sqlContext.sql(querytmp13)

LAC.registerTempTable("LAC")





val querytmp14 =
  """
  SELECT *

  FROM LAC

  WHERE Rn IN (1 ,2)

  """

val #LACT = sqlContext.sql(querytmp14)
#M
#LACT.registerTempTable("#LACT")





val querytmp15 =
  """

  SELECT a.*

    ,B.LactateDate AS SecoundLactDate
    ,B.LactateResult AS SecoundLactateResult

  FROM #LACT AS a
  LEFT JOIN #LACT AS b ON a.EID = B.EID

 AND b.Rn = 2
  WHERE A.Rn = 1

  """

val #LACFIN = sqlContext.sql(querytmp15)

#LACFIN.registerTempTable("#LACFIN")





val querytmp16 =
  """
  SELECT EID

    ,MIN([Activity Date/Time]) AS ActivityDate

    ,DIS.query

    ,CASE DIS.Query

      WHEN 'NVSBPS'

     THEN Dis.[Query Text]

    WHEN 'NVSBPD'

     THEN Dis.[Query Text]

      WHEN 'NVSTEMP'

        THEN Dis.[Query Text]

      WHEN 'NRRRC'

        THEN Dis.[Query Text]
      WHEN 'NVSPULSE'

        THEN Dis.[Query Text]

      END AS EDVitalSigns
    ,CASE DIS.Query

      WHEN 'NVSBPS'

  THEN DIS.[Query Text 2]

WHEN 'NVSBPD'

        THEN DIS.[Query Text 2]

      WHEN 'NVSTEMP'

        THEN DIS.[Query Text 2]
      WHEN 'NRRRC'

  THEN DIS.[Query Text 2]

WHEN 'NVSPULSE'

        THEN DIS.[Query Text 2]
      END AS EDVitalsValues

  FROM QryIntVisitPT_All_Activity AS DIS WITH

  WHERE DIS.Query IN (

     'NVSBPS'

    ,'NVSBPD'

    ,'NVSTEMP'

    ,'NRRRC'

    ,'NVSPULSE'

)

    AND DIS.INSTITUTION IN ("MHR")

GROUP BY Dis.[Query Text]

    ,Dis.[Query Text 2]

    ,DIS.[Activity Date/Time]

    ,DIS.Query

    ,DIS.Account

,DIs.EID

  """
val DIS = sqlContext.sql(querytmp16)

DIS.registerTempTable("DIS")





val querytmp17 =
  """

  SELECT #LACFIN.*

    ,DIS.EDVitalSigns

,DIS.EDVitalsValues

,ROW_NUMBER() OVER (

      PARTITION BY DIS.EID

      ,DIS.query ORDER BY DIS.[ActivityDate] ASC

      ) rn1

  FROM #LACFIN

  INNER JOIN DIS ON dis.EID = #LACFIN.EID

  """
val #final1 = sqlContext.sql(querytmp17)

#final1.registerTempTable("#final1")





val querytmp18 =
  """

  SELECT *

FROM #final1

  WHERE rn1 = 1

  """
val acc = sqlContext.sql(querytmp18)

acc.registerTempTable("acc")





val querytmp19 =
  """

  SELECT *

  FROM acc

  PIVOT(MAX([EDVITALSVALUES]) FOR [EDVitalSigns] IN (
    Temperature
    ,[Blood Pressure Diastolic]

    ,[Blood Pressure Systolic]

    ,[Pulse Rate (Adult)]

,[Respiratory Rate]

    )) AS PVT

  """

val #GrandFinal = sqlContext.sql(querytmp19)

#GrandFinal.registerTempTable("#GrandFinal")





val querytmp20 =
  """

  SELECT *

    ,convert(varchar(30), (datediff(mi, GF.[FirstSepsisDoneDate], GF.OrderDatetime)/ 60))+ ':' +

    convert(varchar(30), (datediff(mi, GF.[FirstSepsisDoneDate], GF.OrderDatetime)% 60)) AS
ElapsedTime

    ,CASE

      WHEN (GF.[First Abx Admin Date] IS NULL)
        OR (GF.OrderDatetime IS NULL)

        THEN 'NULL'

      WHEN ((DATEDIFF(mi, GF.[First Abx Admin Date], GF.OrderDatetime) <=180) OR

        (DATEDIFF(mi, GF.[First Abx Admin Date], GF.OrderDatetime) <=-1440) )

        THEN 'Y' ELSE 'N'

        END AS 'ABX within 3hr of Sepsis OS'

    ,CASE

      WHEN (GF.[First BC] IS NULL)
        OR (GF.[First Abx Admin Date] IS NULL)

        THEN 'NULL'
      ELSE
    ,CASE

      WHEN GF.[First BC] < GF.[First Abx Admin Date]

        THEN 'Y'

      WHEN GF.[First BC] > GF.[First Abx Admin Date]

        THEN 'N'

        END
      END AS 'BC done prior to ABX Admin'
    ,CASE
      WHEN (GF.[LactateDate] IS NULL)

        OR (GF.OrderDateTime IS NULL)

        THEN 'NULL'
      WHEN ((DATEDIFF(mi, GF.[LactateDate], GF.[OrderDatetime]) <=180) OR

      (DATEDIFF(mi, GF.[LactateDate], GF.[OrderDatetime]) <=-360))

      THEN 'Y' ELSE 'N'

      END AS 'Lac within 3hr of Sepsis OS'
  FROM #GrandFinal AS GF

LEFT JOIN (

      ) Sub ON GF.EID = Sub.SEID
  WHERE rns = 1
    OR RNS IS NULL

  """

val Report = sqlContext.sql(querytmp20)

Report.registerTempTable("Report")











//     test sfuff below this line




import java.sql.Timestamp


import org.apache.spark.sql.DataFrame

import org.apache.spark.sql.functions._

import org.apache.spark.sql.types.{DataType, LongType}



def convertToDateTime: Long => Timestamp = new Timestamp(_)



/**

  * If the given column in the data frame is of type bigint then it is transformed to timestamp.

  *

  * @param columnName Name of the date column
  * @param df         Data frame

  * @return transformed column

  */


def fixDateTimeColumn(columnName: String, df: DataFrame) = findColumn(df, columnName, LongType)

  .map(udf(convertToDateTime).apply(_))

  .getOrElse(df.col(columnName))



def findColumn(df: DataFrame, colName: String, colType: DataType) = {

  val existingFieldStruct = df.schema.find(f => {

    f.name == colName && f.dataType == colType

  })
  existingFieldStruct.map(structField => df.col(structField.name))
}



/** val hdfs_path = "hdfs://10.0.100.252:8020/user/hdfs/stjoe/amalga/sqooped/2016/09/01" */

val hdfs_path = "file:///Users/jimmarczyk/01/"




val VISITPT_ALL_DIS = sqlContext.read.parquet(s"$hdfs_path/azViews/VISITPT_ALL_DIS/*.parquet")

VISITPT_ALL_DIS

  .withColumn("AdmitDateTime_f", fixDateTimeColumn("AdmitDateTime", VISITPT_ALL_DIS))

  .withColumn("DischargeDateTime_f", fixDateTimeColumn("DischargeDateTime", VISITPT_ALL_DIS))

  .withColumn("ERAdmitDtTm_f", fixDateTimeColumn("ERAdmitDtTm", VISITPT_ALL_DIS))

  .withColumn("LastIPVisitDtTm_f", fixDateTimeColumn("LastIPVisitDtTm", VISITPT_ALL_DIS))

  .drop("AdmitDateTime")

  .drop("DischargeDateTime")

  .drop("ERAdmitDtTm")

  .drop("LastIPVisitDtTm")

  .withColumnRenamed("AdmitDateTime_f", "AdmitDateTime")

  .withColumnRenamed("DischargeDateTime_f", "DischargeDateTime")

  .withColumnRenamed("ERAdmitDtTm_f", "ERAdmitDtTm")

  .withColumnRenamed("LastIPVisitDtTm_f", "LastIPVisitDtTm")

  .registerTempTable("VISITPT_ALL_DIS")

val ABS_DG1601 = sqlContext.read.parquet(s"$hdfs_path/azABS/ABS_DG1601/*.parquet")

ABS_DG1601.registerTempTable("ABS_DG1601")





val querytmp0 =
  """

  SELECT DISTINCT COUNT(DIS.Name) // , DF.DxCode, DIS.DischargeDateTime, DIS.PatientType, DIS.ServicingFacility, DIS.Institution, DIS.AccountStatus

  FROM VISITPT_ALL_DIS DIS

  JOIN ABS_DG1601 DF ON DIS.EID = DF.EID

  WHERE DF.DxCode IN (

     '785.52'

    ,'995.91'

    ,'995.92'

    ,'R65.21'

,'A41.9'

    ,'R65.20'

,'A021'

    ,'A227'

    ,'A267'

    ,'A327'

    ,'A400'

    ,'A401'

    ,'A403'

    ,'A408'

    ,'A409'

    ,'A4101'

    ,'A4102'

    ,'A411'

    ,'A412'

,'A413'

,'A414'

    ,'A4150'

    ,'A4151'

    ,'A4152'

    ,'A4153'

,'A4159'

    ,'A4181'

,'A4189'

    ,'A419'

    ,'A427'

,'A5486'

    ,'B377'

    ,'R6520'

,'R6521')
AAND DIS.DischargeDateTime BETWEEN '2016-01-01 00:00:00.0' AND '2016-01-31 23:59:00.0'

AND DIS.PatientType = 'IN'

AND DIS.ServicingFacility = 'SMM'

AND DIS.Institution = 'MHR'

AND DIS.AccountStatus = 'DIS'

  """
val freebase = sqlContext.sql(querytmp0)

freebase.registerTempTable("freebase")









val querytmp0 =
  """

  SELECT COUNT(DIS.EID)

ROM VISITPT_ALL_DIS DIS

  JOIN ABS_DG1601 DF ON DIS.EID = DF.EID

  WHERE DIS.Institution = 'MHR'

    AND DIS.ServicingFacility = 'SMM'

    AND DIS.PatientType = 'IN'

    AND DIS.DischargeDateTime BETWEEN '2016-01-01 00:00:00.0' AND '2016-01-31 23:59:00.0'

    AND DIS.AccountStatus = 'DIS'

  """

val freebase = sqlContext.sql(querytmp0)

freebase.registerTempTable("freebase")







val querytmp0 =
  """
  SELECT DISTINCT COUNT(DIS.Name)

FROM ABS_DG1601 DF

  WHERE DF.DxCode IN (

     '785.52'
    ,'995.91'

    ,'995.92'

    ,'R65.21'

    ,'A41.9'

,'R65.20'

    ,'A021'

    ,'A227'

    ,'A267'

,'A327'

    ,'A400'

,'A401'

    ,'A403'

    ,'A408'

    ,'A409'

    ,'A4101'

,'A4102'

    ,'A411'

    ,'A412'

,'A413'
    ,'A414'

    ,'A4150'

    ,'A4151'

    ,'A4152'

    ,'A4153'

    ,'A4159'

    ,'A4181'

    ,'A4189'

,'A419'

    ,'A427'

    ,'A5486'

    ,'B377'

    ,'R6520'

    ,'R6521'

    )

  """
val freebase = sqlContext.sql(querytmp0)

freebase.registerTempTable("freebase")






//val hdfs_path = "file:///Users/jimmarczyk/01/"



val ABS_DG1601 = sqlContext.read.parquet(s"$hdfs_path/azABS/ABS_DG1601/*.parquet")

ABS_DG1601.registerTempTable("ABS_DG1601")



val VISITPT_ALL_DIS = sqlContext.read.parquet(s"$hdfs_path/azViews/VISITPT_ALL_DIS/*.parquet")

VISITPT_ALL_DIS.registerTempTable("VISITPT_ALL_DIS")



//val hdfs_path = "file:///Users/jimmarczyk/01/"

val hdfs_path = "hdfs://10.0.100.252:8020/user/hdfs/stjoe/amalga/sqooped/2016/09/01"




val PID601 = sqlContext.read.parquet(s"$hdfs_path/azADT/PID601/*.parquet")

PID601.registerTempTable("PID601")



val querytmp0 =
  """
  select count(EID)

from PID601

  """


val freebase = sqlContext.sql(querytmp0)

freebase.registerTempTable("freebase")



val hdfs_path = "file:///Users/jimmarczyk/01/"

//val hdfs_path = "hdfs://10.0.100.252:8020/user/hdfs/stjoe/amalga/sqooped/2016/09/01"



val LABVISITPT_ALL_DIS = sqlContext.read.parquet(s"$hdfs_path/azViews/LABVISITPT_ALL_DIS/*.parquet")

LABVISITPT_ALL_DIS

  .withColumn("CollectedDateTime_f", fixDateTimeColumn("CollectedDateTime", LABVISITPT_ALL_DIS))

  .withColumn("DischargeDateTime_f", fixDateTimeColumn("DischargeDateTime", LABVISITPT_ALL_DIS))

  .drop("CollectedDateTime")

  .drop("DischargeDateTime")

  .withColumnRenamed("CollectedDateTime_f", "CollectedDateTime")

  .withColumnRenamed("DischargeDateTime_f", "DischargeDateTime")

  .registerTempTable("LABVISITPT_ALL_DIS")



import org.apache.spark.sql.functions.rank
import org.apache.spark.sql.expressions.Window

val w = Window.orderBy($"value")

val df = sc.parallelize(Seq(
  ("a", 5), ("b", 10), ("c", 5), ("d", 6)
)).toDF("user", "value")

df.select($"user", rank.over(w).alias("rank")).show

