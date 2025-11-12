USE --<INSERT DB_NAME>--
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TRIGGER [dbo].[TR_LOG_ARCHVL_PRG_TBL_DTL] ON [dbo].[LOG_ARCHVL_PRG_TBL_DTL]  
    AFTER INSERT, UPDATE, DELETE
AS

/********************************************************************************************************************
	Name     :  [dbo].[TR_LOG_ARCHVL_PRG_TBL_DTL]
	Author   :  
	Purpose  :  Trigger is used to maintain history and auto-populate the archive table & view details
    
	*********************************************************************************
	* Change Date        Change By           Change DSC
	* -----------        --------------      -----------------------------------------

*********************************************************************************************************************/
	IF @@ROWCOUNT = 0 
	BEGIN
		RETURN;
	END

	DECLARE   
		@l_currdate datetime = GETDATE(),  
		@v_max_date datetime = '9999-DEC-31';   

	BEGIN TRY
		-- History Table Trigger Logic
		IF EXISTS(SELECT * FROM DELETED)  
		BEGIN  
			UPDATE HstTbl  
			SET HstTbl.HIST_EFF_TO_DTM = @l_currdate  
			FROM dbo.LOG_ARCHVL_PRG_TBL_DTL_HIST HstTbl  
			INNER JOIN DELETED del  
			ON HstTbl.LOG_ARCHVL_PRG_TBL_DTL_SID = del.LOG_ARCHVL_PRG_TBL_DTL_SID  
			AND HstTbl.HIST_EFF_TO_DTM = @v_max_date;  
		END  

		INSERT INTO dbo.LOG_ARCHVL_PRG_TBL_DTL_HIST  
			(LOG_ARCHVL_PRG_TBL_DTL_SID, SRT_ORDR, [DB_NAME], [SCHEMA], LOG_TBL_NM, IS_PURGE, IS_ARCHV, ARCHV_DB_NAME, ARCHV_SCHEMA, ARCHV_TBL_NM, VIEW_NM, 
			JSON_COND, ACTV_IND, [STATUS], LST_RUN, CRE_DTM, CRE_EMP_WWID, CHG_DTM, CHG_EMP_WWID, HIST_EFF_FR_DTM, HIST_EFF_TO_DTM)  
		SELECT new.LOG_ARCHVL_PRG_TBL_DTL_SID, new.SRT_ORDR, new.[DB_NAME], new.[SCHEMA], new.LOG_TBL_NM, new.IS_PURGE, new.IS_ARCHV, new.ARCHV_DB_NAME, 
			new.ARCHV_SCHEMA, new.ARCHV_TBL_NM, new.VIEW_NM, new.JSON_COND, new.ACTV_IND, new.[STATUS], new.LST_RUN, new.CRE_DTM, new.CRE_EMP_WWID, new.CHG_DTM, 
			new.CHG_EMP_WWID, @l_currdate, @v_max_date  
		FROM INSERTED new

		-- Logic To Auto-populate Archive Table & View Details 
		IF EXISTS (SELECT TOP 1 1 FROM INSERTED WHERE ACTV_IND = 1 AND IS_ARCHV = 1)
		BEGIN 
			BEGIN TRANSACTION
				UPDATE tbl
				SET tbl.ARCHV_DB_NAME = ISNULL(ins.ARCHV_DB_NAME, 'MYDEALS_FILE'),
					tbl.ARCHV_SCHEMA = ISNULL(ins.ARCHV_SCHEMA, 'DBO'),
					tbl.ARCHV_TBL_NM = ins.LOG_TBL_NM + '_ARCHV',
					tbl.VIEW_NM = 'MYDEALS.DBO.VW_' + ins.LOG_TBL_NM
				FROM dbo.LOG_ARCHVL_PRG_TBL_DTL tbl
				INNER JOIN INSERTED ins 
				ON tbl.LOG_ARCHVL_PRG_TBL_DTL_SID = ins.LOG_ARCHVL_PRG_TBL_DTL_SID
				WHERE ins.IS_ARCHV = 1 AND ins.ACTV_IND = 1;	 
				
				-- Re-insert into history table after updating archive details
				UPDATE HstTbl  
				SET HstTbl.HIST_EFF_TO_DTM = @l_currdate  
				FROM dbo.LOG_ARCHVL_PRG_TBL_DTL_HIST HstTbl  
				INNER JOIN INSERTED ins  
				ON HstTbl.LOG_ARCHVL_PRG_TBL_DTL_SID = ins.LOG_ARCHVL_PRG_TBL_DTL_SID  
				AND HstTbl.HIST_EFF_TO_DTM = @v_max_date;  
				
				INSERT INTO dbo.LOG_ARCHVL_PRG_TBL_DTL_HIST  
					(LOG_ARCHVL_PRG_TBL_DTL_SID, SRT_ORDR, [DB_NAME], [SCHEMA], LOG_TBL_NM, IS_PURGE, IS_ARCHV, ARCHV_DB_NAME, ARCHV_SCHEMA, ARCHV_TBL_NM, VIEW_NM, 
					JSON_COND, ACTV_IND, [STATUS], LST_RUN, CRE_DTM, CRE_EMP_WWID, CHG_DTM, CHG_EMP_WWID, HIST_EFF_FR_DTM, HIST_EFF_TO_DTM)  
				SELECT tbl.LOG_ARCHVL_PRG_TBL_DTL_SID, tbl.SRT_ORDR, tbl.[DB_NAME], tbl.[SCHEMA], tbl.LOG_TBL_NM, tbl.IS_PURGE, tbl.IS_ARCHV, tbl.ARCHV_DB_NAME, 
					tbl.ARCHV_SCHEMA, tbl.ARCHV_TBL_NM, tbl.VIEW_NM, tbl.JSON_COND, tbl.ACTV_IND, tbl.[STATUS], tbl.LST_RUN, tbl.CRE_DTM, tbl.CRE_EMP_WWID, tbl.CHG_DTM, 
					tbl.CHG_EMP_WWID, @l_currdate, @v_max_date  
				FROM dbo.LOG_ARCHVL_PRG_TBL_DTL tbl
				INNER JOIN INSERTED ins 
				ON tbl.LOG_ARCHVL_PRG_TBL_DTL_SID = ins.LOG_ARCHVL_PRG_TBL_DTL_SID
            COMMIT TRANSACTION
        END
	END TRY  
	BEGIN CATCH  
		IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
		EXEC dbo.PR_CUSTOM_ERRMSG;  
	END CATCH
