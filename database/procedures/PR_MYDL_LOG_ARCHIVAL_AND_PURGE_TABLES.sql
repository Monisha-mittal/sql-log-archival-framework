USE --<INSERT DB_NAME>--
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

EXEC dbo.PR_ENSURE_EXIST 'P', 'dbo.PR_MYDL_LOG_ARCHIVAL_AND_PURGE_TABLES'
GO

ALTER PROCEDURE [dbo].[PR_MYDL_LOG_ARCHIVAL_AND_PURGE_TABLES] 
AS 

/************************************************************************************
	Name     :  [dbo].[PR_MYDL_LOG_ARCHIVAL_AND_PURGE_TABLES]
	Author   :  Monisha Mittal 
	Purpose  :  Proc is used to Archive and Purge Log Tables of given Database.
    
	*********************************************************************************
	* Change Date        Change By           Change DSC
	* -----------        --------------      -----------------------------------------

*************************************************************************************/

BEGIN
	SET NOCOUNT ON
	BEGIN TRY
		IF EXISTS (SELECT TOP 1 1 FROM [DBO].[LOG_ARCHVL_PRG_TBL_DTL] WHERE ACTV_IND = 1)
		BEGIN
			DROP TABLE IF EXISTS #TempTable;
			CREATE TABLE #TempTable (
				[ID] [int] IDENTITY(1,1),
				[LOG_ARCHVL_PRG_TBL_DTL_SID] [int],
				[DB_NAME] [varchar](128),
				[SCHEMA] [varchar](128),
				[LOG_TBL_NM] [nvarchar](500),
				[IS_PURGE] BIT,
				[IS_ARCHV] BIT,
				[ARCHV_DB_NAME] [varchar](128),
				[ARCHV_SCHEMA] [varchar](128),
				[ARCHV_TBL_NM] [nvarchar](500),
				[JSON_COND] [nvarchar](MAX)
			)

			INSERT INTO #TempTable ([LOG_ARCHVL_PRG_TBL_DTL_SID], [DB_NAME], [SCHEMA], [LOG_TBL_NM], [IS_PURGE], [IS_ARCHV], [ARCHV_DB_NAME], [ARCHV_SCHEMA], [ARCHV_TBL_NM], [JSON_COND])
			SELECT [LOG_ARCHVL_PRG_TBL_DTL_SID], [DB_NAME], [SCHEMA], [LOG_TBL_NM], [IS_PURGE], [IS_ARCHV], [ARCHV_DB_NAME], [ARCHV_SCHEMA], [ARCHV_TBL_NM], [JSON_COND]
			FROM [DBO].[LOG_ARCHVL_PRG_TBL_DTL]
			WHERE ACTV_IND = 1 ORDER BY SRT_ORDR

			DECLARE @Counter INT = 1, @TotalCount INT, @ENVT VARCHAR(20) = dbo.FN_GET_ENV()

			-- JSON FORMAT TO FOLLOW: {
			--"ARCHV_DTL": {"ARCHV_IN_DAYS": 90, "DATE_FILTER":"CHG_DTM", "ARCHVL_PROC": "", "COLUMN_FILTER": "IS_PRCSSD = 2 AND STS=''Processed''"}, 
			--"PURGE_DTL": {"PURGE_IN_DAYS": 365, "DATE_FILTER":"CHG_DTM", "COLUMN_FILTER": "IS_PRCSSD = 2 AND STS=''Processed''"}}

			SELECT @TotalCount = MAX(ID) FROM #TempTable
			WHILE (@Counter <= @TotalCount)
			BEGIN
			BEGIN TRY
				BEGIN TRANSACTION	
					DECLARE @DATE DATETIME = GETDATE(), @SUBJ NVARCHAR(150), @HTMLTBL NVARCHAR(MAX), @Comment NVARCHAR(MAX) = '', @LogTableExists INT = 0, 
					@ArchiveTableExists INT = 0, @ViewExists INT = 0

					DECLARE @LOG_ARCHVL_PRG_TBL_DTL_SID INT, @DB_NAME VARCHAR(128), @SCHEMA VARCHAR(128), @LOG_TBL_NM NVARCHAR(500), @IS_PURGE BIT,
					@IS_ARCHV BIT, @ARCHV_DB_NAME VARCHAR(128), @ARCHV_SCHEMA VARCHAR(128), @ARCHV_TBL_NM NVARCHAR(500), @JSON_COND NVARCHAR(MAX)

					-- Get values for the current LOG_ARCHVL_PRG_TBL_DTL_SID
					SELECT @LOG_ARCHVL_PRG_TBL_DTL_SID = [LOG_ARCHVL_PRG_TBL_DTL_SID],
						@DB_NAME = [DB_NAME],
						@SCHEMA = [SCHEMA],
						@LOG_TBL_NM = [LOG_TBL_NM],
						@IS_PURGE = [IS_PURGE],
						@IS_ARCHV = [IS_ARCHV],
						@ARCHV_DB_NAME = [ARCHV_DB_NAME],
						@ARCHV_SCHEMA = [ARCHV_SCHEMA],
						@ARCHV_TBL_NM = [ARCHV_TBL_NM],
						@JSON_COND = [JSON_COND]
					FROM #TempTable
					WHERE ID = @Counter

					--UPDATING STATUS AS RUNNING IN CONFIG TABLE FOR RESPECTIVE LOG TABLE
					UPDATE DBO.LOG_ARCHVL_PRG_TBL_DTL
					SET [STATUS] = 'RUNNING',
					[CHG_DTM] = @DATE
					WHERE LOG_ARCHVL_PRG_TBL_DTL_SID = @LOG_ARCHVL_PRG_TBL_DTL_SID 					

					-- If the log table does not exist in the specified database, throw error
                    DECLARE @CheckLogTable NVARCHAR(MAX) = 'SELECT @LogTableExists = COUNT(1) FROM ' + @DB_NAME + '.INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = @LOG_TBL_NM AND TABLE_SCHEMA = @SCHEMA';
                    EXEC sp_executesql @CheckLogTable, N'@LogTableExists INT OUTPUT, @LOG_TBL_NM NVARCHAR(500), @SCHEMA VARCHAR(128)', @LogTableExists OUTPUT, @LOG_TBL_NM, @SCHEMA;
                    IF (@LogTableExists = 0)
                    BEGIN
                        SET @Comment = @SCHEMA + '.' + @LOG_TBL_NM + ' Table does not exist in ' + @DB_NAME + ' Database';
						RAISERROR(@Comment, 16, 1); 
                    END
					
					IF (@IS_ARCHV = 1) -- Archival Only When IS_ARCHV flag is on
					BEGIN	
						DECLARE @ArchiveConditions NVARCHAR(MAX) = JSON_QUERY(@JSON_COND, '$.ARCHV_DTL')
						IF (@ArchiveConditions IS NULL OR @ArchiveConditions = '') -- If ARCHV_DTL value does not exist in JSON, throw error
						BEGIN
							SET @Comment = 'For Table ' + @DB_NAME + '.' + @SCHEMA + '.' + @LOG_TBL_NM + ', ''ARCHV_DTL'' Key is required in JSON when IS_ARCHV flag is ON';
							RAISERROR(@Comment, 16, 1); 
						END

						 -- If ARCHVL_PROC value is passed in JSON, then execute the archival proc
						DECLARE @PROC_ARCHVL NVARCHAR(MAX) = JSON_VALUE(JSON_QUERY(@JSON_COND, '$.ARCHV_DTL'), '$.ARCHVL_PROC');						
						IF(@PROC_ARCHVL IS NOT NULL OR @PROC_ARCHVL != '')
						BEGIN
							EXEC sp_executesql @PROC_ARCHVL;
							SET @DATE = GETDATE()
							UPDATE DBO.LOG_ARCHVL_PRG_TBL_DTL
							SET [STATUS] = 'COMPLETED',
								[LST_RUN] = @DATE,
								[CHG_DTM] = @DATE
							WHERE LOG_ARCHVL_PRG_TBL_DTL_SID = @LOG_ARCHVL_PRG_TBL_DTL_SID 
					
							SET @Counter = @Counter + 1;
							COMMIT TRANSACTION;
							CONTINUE;
						END
						
						-- NON-ARCHIVAL PROC LOGIC STARTS HERE
						----- Extract values from JSON and set whereclause for archival
						DECLARE @ArchiveWhereClause NVARCHAR(MAX), @ArchvInDays INT, @ArchvDateFilter NVARCHAR(128), @ArchvColFilter NVARCHAR(MAX), @ArchiveDate DATETIME;
						SET @ArchvInDays = TRY_CAST(JSON_VALUE(@ArchiveConditions, '$.ARCHV_IN_DAYS') AS INT)
						SET @ArchvDateFilter = ISNULL(JSON_VALUE(@ArchiveConditions, '$.DATE_FILTER'), '')
						SET @ArchvColFilter = ISNULL(JSON_VALUE(@ArchiveConditions, '$.COLUMN_FILTER'), '')							
								
						IF (@ArchvInDays <= 0)
						BEGIN
							SET @Comment = '''ARCHV_IN_DAYS'' key requires positive integer value in JSON.'
							RAISERROR(@Comment, 16, 1); 
						END
						ELSE IF (@ArchvInDays > 0 AND @ArchvDateFilter = '')
						BEGIN
							SET @Comment = 'Both ''ARCHV_IN_DAYS'' and ''DATE_FILTER'' key must be provided together with correct value in JSON.'
							RAISERROR(@Comment, 16, 1); 
						END
						ELSE IF (@ArchvInDays > 0 AND @ArchvDateFilter != '')
						BEGIN
							SET @ArchiveDate = DATEADD(DAY, -@ArchvInDays, @DATE)
							SET @ArchiveWhereClause = @ArchvDateFilter + ' < @ArchiveDate'
							IF (@ArchvColFilter != '')
							BEGIN
								SET @ArchiveWhereClause = @ArchiveWhereClause + ' AND ' + @ArchvColFilter
							END	
						END
						ELSE IF (@ArchvColFilter != '')
						BEGIN
							SET @ArchiveWhereClause = @ArchvColFilter
						END			             
						ELSE
						BEGIN
							SET @Comment = 'Invalid JSON found for Archival of ' + @DB_NAME + '.' + @SCHEMA + '.' + @LOG_TBL_NM +  ' Table'
							RAISERROR(@Comment, 16, 1); 
						END
							
						-- Get the column list of the log table
						DECLARE @ColumnList NVARCHAR(MAX) = ''
						DECLARE @SQL NVARCHAR(MAX) = 'SELECT @ColumnList = STRING_AGG(''['' + COLUMN_NAME + '']'', '', '') FROM ' + @DB_NAME + '.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = @LOG_TBL_NM AND TABLE_SCHEMA = @SCHEMA'
						EXEC sp_executesql @SQL, N'@ColumnList NVARCHAR(MAX) OUTPUT, @LOG_TBL_NM NVARCHAR(500), @SCHEMA VARCHAR(128)', @ColumnList OUTPUT, @LOG_TBL_NM, @SCHEMA;
                  
						-- If the archive table does not exist in the archive database, create it
						DECLARE @CheckArchiveTable NVARCHAR(MAX) = 'SELECT @ArchiveTableExists = COUNT(1) FROM ' + @ARCHV_DB_NAME + '.INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = @ARCHV_TBL_NM AND TABLE_SCHEMA = @ARCHV_SCHEMA'
						EXEC sp_executesql @CheckArchiveTable, N'@ArchiveTableExists INT OUTPUT, @ARCHV_TBL_NM NVARCHAR(500), @ARCHV_SCHEMA VARCHAR(128)', @ArchiveTableExists OUTPUT, @ARCHV_TBL_NM, @ARCHV_SCHEMA;							
						IF (@ArchiveTableExists = 0)
						BEGIN								
							EXEC('SELECT * INTO ' + @ARCHV_DB_NAME + '.' + @ARCHV_SCHEMA + '.' + @ARCHV_TBL_NM + ' FROM ' + @DB_NAME + '.' + @SCHEMA + '.' + @LOG_TBL_NM + ' WHERE 1=0; ' +
							'ALTER TABLE ' + @ARCHV_DB_NAME + '.' + @ARCHV_SCHEMA + '.' + @ARCHV_TBL_NM + ' ADD LOG_ARCHV_DTM DATETIME;') 	
							
							-- If the view does not exist in the database, create it
							DECLARE @CheckView NVARCHAR(MAX) = 'SELECT @ViewExists = COUNT(1) FROM ' + @ENVT + '.INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = ''VW_' + @LOG_TBL_NM + ''''
							EXEC sp_executesql @CheckView, N'@ViewExists INT OUTPUT, @LOG_TBL_NM NVARCHAR(500)', @ViewExists OUTPUT, @LOG_TBL_NM;
							IF (@ViewExists = 0)
							BEGIN		
								EXEC('USE ' + @ENVT + '; ')
								EXEC('SET ANSI_NULLS ON; ')
								EXEC('SET QUOTED_IDENTIFIER ON; ')
								EXEC('EXEC [dbo].[PR_ENSURE_EXIST] ''VW'', ''dbo.VW_' + @LOG_TBL_NM + '''; ')
								EXEC('ALTER VIEW dbo.VW_' + @LOG_TBL_NM + ' AS 
									/* Created by System */
									SELECT ' + @ColumnList + ', CAST(NULL AS DATETIME) AS LOG_ARCHV_DTM ' +
									'FROM ' + @DB_NAME + '.' + @SCHEMA + '.' + @LOG_TBL_NM + 
									' UNION ' +
									'SELECT ' + @ColumnList + ', LOG_ARCHV_DTM ' +
									'FROM ' + @ARCHV_DB_NAME + '.' + @ARCHV_SCHEMA + '.' + @ARCHV_TBL_NM + '; ') 	
							END

							-- Send email notification
							SET @SUBJ = @ENVT + ' - Add code in Git for object created by system'        
							SET @HTMLTBL = N'Hi,<br>
								<br>Please add code in Git for the mentioned object created by system while executing the proc PR_MYDL_LOG_ARCHIVAL_AND_PURGE_TABLES:- <br>' + 
								CASE 
									WHEN @ArchiveTableExists = 0 AND @ViewExists = 0 THEN 'Archive Table - ' + @ARCHV_DB_NAME + '.' + @ARCHV_SCHEMA + '.' + @ARCHV_TBL_NM + '<br>View - ' + @ENVT + '.dbo.VW_' + @LOG_TBL_NM
									WHEN @ArchiveTableExists = 0 THEN 'Archive Table - ' + @ARCHV_DB_NAME + '.' + @ARCHV_SCHEMA + '.' + @ARCHV_TBL_NM
								END + '<br><br>
								Ignore if already added.<br>
								<br>
								Regards,<br>        
								Team <app> <br>' 
							
							EXEC dbo.PR_SQL_DB_MAIL
								@from_user_email = '<app>@intel.com'     
								,@to_user_email = '<app>.all.developers@intel.com'        
								,@subject = @SUBJ  
								,@message = @HTMLTBL
								,@priority = 'High'        
						END
						
						-- Archiving the logs from log table to archive table. Then deleting those logs from log table.
						DECLARE @ArchiveSQL NVARCHAR(MAX) = '
						BEGIN TRY
							BEGIN TRANSACTION;
								IF EXISTS (SELECT 1 FROM ' + @ARCHV_DB_NAME + '.SYS.IDENTITY_COLUMNS WHERE OBJECT_NAME(OBJECT_ID, DB_ID(''' + @ARCHV_DB_NAME + ''')) = ''' + @ARCHV_TBL_NM + ''')
								BEGIN
									SET IDENTITY_INSERT ' + @ARCHV_DB_NAME + '.' + @ARCHV_SCHEMA + '.' + @ARCHV_TBL_NM + ' ON; 
									INSERT INTO ' + @ARCHV_DB_NAME + '.' + @ARCHV_SCHEMA + '.' + @ARCHV_TBL_NM + ' (' + @ColumnList + ', LOG_ARCHV_DTM) 
									SELECT ' + @ColumnList + ', CONVERT(NVARCHAR, @DATE, 120) AS LOG_ARCHV_DTM                        
									FROM ' + @DB_NAME + '.' + @SCHEMA + '.' + @LOG_TBL_NM + ' WHERE ' + @ArchiveWhereClause + '
									DELETE FROM ' + @DB_NAME + '.' + @SCHEMA + '.' + @LOG_TBL_NM + ' WHERE ' + @ArchiveWhereClause + '
									SET IDENTITY_INSERT ' + @ARCHV_DB_NAME + '.' + @ARCHV_SCHEMA + '.' + @ARCHV_TBL_NM + ' OFF; 
								END
								ELSE
								BEGIN
									INSERT INTO ' + @ARCHV_DB_NAME + '.' + @ARCHV_SCHEMA + '.' + @ARCHV_TBL_NM + ' (' + @ColumnList + ', LOG_ARCHV_DTM) 
									SELECT ' + @ColumnList + ', CONVERT(NVARCHAR, @DATE, 120) AS LOG_ARCHV_DTM                        
									FROM ' + @DB_NAME + '.' + @SCHEMA + '.' + @LOG_TBL_NM + ' WHERE ' + @ArchiveWhereClause + '
									DELETE FROM ' + @DB_NAME + '.' + @SCHEMA + '.' + @LOG_TBL_NM + ' WHERE ' + @ArchiveWhereClause + '
								END
							COMMIT TRANSACTION;
						END TRY
						BEGIN CATCH                      
							ROLLBACK TRANSACTION;
							EXEC dbo.PR_CUSTOM_ERRMSG
						END CATCH'
						EXEC sp_executesql @ArchiveSQL, N'@ArchiveDate DATETIME, @DATE DATETIME', @ArchiveDate, @DATE			
					END					
				
					IF (@IS_PURGE = 1) -- PURGING LOGIC STARTS HERE
					BEGIN						
						DECLARE @PurgeConditions NVARCHAR(MAX), @PurgeWhereClause NVARCHAR(MAX), @PurgeInDays INT, @PurgeDateFilter NVARCHAR(128), @PurgeColFilter NVARCHAR(MAX), @PurgeDate DATETIME;

						SET @PurgeConditions = JSON_QUERY(@JSON_COND, '$.PURGE_DTL')
						IF (@PurgeConditions IS NULL OR @PurgeConditions = '') -- If PURGE_DTL value does not exist in JSON, throw error
						BEGIN
							SET @Comment = 'For Table ' + @DB_NAME + '.' + @SCHEMA + '.' + @LOG_TBL_NM + ', ''PURGE_DTL'' Key is required in JSON when IS_PURGE flag is ON';
							RAISERROR(@Comment, 16, 1); 
						END

						-- Extract values from JSON and set whereclause for purging
						SET @PurgeInDays = TRY_CAST(JSON_VALUE(@PurgeConditions, '$.PURGE_IN_DAYS') AS INT)
						SET @PurgeDateFilter = ISNULL(JSON_VALUE(@PurgeConditions, '$.DATE_FILTER'), '')
						SET @PurgeColFilter = ISNULL(JSON_VALUE(@PurgeConditions, '$.COLUMN_FILTER'), '')							
														
						IF (@PurgeInDays <= 0)
						BEGIN
							SET @Comment = '''PURGE_IN_DAYS'' key requires positive integer value in JSON.'
							RAISERROR(@Comment, 16, 1); 
						END
						ELSE IF (@PurgeInDays > 0 AND @PurgeDateFilter = '')
						BEGIN
							SET @Comment = 'Both ''PURGE_IN_DAYS'' and ''DATE_FILTER'' key must be provided together with correct value in JSON.'
							RAISERROR(@Comment, 16, 1); 
						END
						ELSE IF (@PurgeInDays > 0 AND @PurgeDateFilter != '')
						BEGIN
							SET @PurgeDate = DATEADD(DAY, -@PurgeInDays, @DATE)
							SET @PurgeWhereClause = @PurgeDateFilter + ' < @PurgeDate'
							IF (@PurgeColFilter != '')
							BEGIN
								SET @PurgeWhereClause = @PurgeWhereClause + ' AND ' + @PurgeColFilter
							END	
						END
						ELSE IF (@PurgeColFilter != '')
						BEGIN
							SET @PurgeWhereClause = @PurgeColFilter
						END			             
						ELSE
						BEGIN
							SET @Comment = 'Invalid JSON found for Purging of ' + @DB_NAME + '.' + @SCHEMA + '.' + @LOG_TBL_NM +  ' Table'
							RAISERROR(@Comment, 16, 1); 
						END
						
						--IF NOT ARCHIVAL THEN PURGE FROM LOG TABLE
						IF (@IS_ARCHV = 0)	
						BEGIN
							DECLARE @PurgeLogSQL NVARCHAR(MAX) = 'DELETE FROM ' + @DB_NAME + '.' + @SCHEMA + '.' + @LOG_TBL_NM + ' WHERE ' + @PurgeWhereClause
							EXEC sp_executesql @PurgeLogSQL, N'@PurgeDate DATETIME', @PurgeDate
						END
						ELSE
						--IF ARCHIVAL THEN PURGE FROM ARCHIVAL TABLE
						BEGIN
							DECLARE @PurgeArchiveSQL NVARCHAR(MAX) = 'DELETE FROM ' + @ARCHV_DB_NAME + '.' + @ARCHV_SCHEMA + '.' + @ARCHV_TBL_NM + ' WHERE ' + @PurgeWhereClause
							EXEC sp_executesql @PurgeArchiveSQL, N'@PurgeDate DATETIME', @PurgeDate
						END
					END
					SET @DATE = GETDATE()
					--UPDATING LAST RUN STATUS IN CONFIG TABLE
					UPDATE DBO.LOG_ARCHVL_PRG_TBL_DTL
					SET [STATUS] = 'COMPLETED',
						[LST_RUN] = @DATE,
						[CHG_DTM] = @DATE
					WHERE LOG_ARCHVL_PRG_TBL_DTL_SID = @LOG_ARCHVL_PRG_TBL_DTL_SID 
						
					SET @Counter = @Counter + 1
				COMMIT TRANSACTION;
			END TRY
			BEGIN CATCH                
				-- Handle errors for the current row and move to the next row
                IF @@TRANCOUNT > 0
                BEGIN
                    ROLLBACK TRANSACTION
                END

				SET @DATE = GETDATE()		

				-- Capture the error message
				DECLARE @SysErrMsg NVARCHAR(4000) = ERROR_MESSAGE();
    
                UPDATE DBO.LOG_ARCHVL_PRG_TBL_DTL
                SET [ACTV_IND] = 0,
				[STATUS] = 'FAILED - ' + 
					CASE 
                       WHEN @Comment IS NOT NULL AND @Comment <> '' THEN @Comment
                       ELSE @SysErrMsg
                   END,
				[LST_RUN] = @DATE,
				[CHG_DTM] = @DATE
                WHERE LOG_ARCHVL_PRG_TBL_DTL_SID = @LOG_ARCHVL_PRG_TBL_DTL_SID	
				
				-- Send email notification for failure
				SET @SUBJ = @ENVT + ' - Log archival or purge failed for table ' + @LOG_TBL_NM;
				SET @HTMLTBL = N'Hi,<br><br>The log archival or purging process failed for the table ' + @DB_NAME + '.' + @SCHEMA + '.' + @LOG_TBL_NM + '.<br><br>Please check dbo.DB_ERR_LOG for more details.<br><br>Regards,<br>Team <app><br>';
				EXEC dbo.PR_SQL_DB_MAIL
					@from_user_email = '<app>@intel.com' ,        
					@to_user_email = '<app>.all.developers@intel.com',
					@subject = @SUBJ,
					@message = @HTMLTBL,
					@priority = 'High';		
				
                SET @Counter = @Counter + 1
				EXEC dbo.PR_CUSTOM_ERRMSG 'CONTINUE'
				
            END CATCH
			END
			DROP TABLE #TempTable
		END        
	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0
		BEGIN
			ROLLBACK TRANSACTION
		END
		EXEC dbo.PR_CUSTOM_ERRMSG
	END CATCH
END