USE [Warehouse]

GO

SET ANSI_NULLS ON

GO

SET QUOTED_IDENTIFIER ON

GO

CREATE
	OR

ALTER PROCEDURE [dbo].[Update fPreorder History]
AS
BEGIN
	SET XACT_ABORT ON;

	SET NOCOUNT ON;

	DECLARE @Timestamp DATETIME2(3)
		,@Level NVARCHAR(10)
		,@File_Name NVARCHAR(270)
		,@Table NVARCHAR(50)
		,@Action NVARCHAR(500)
		,@Start_Date DATE
		,@End_Date DATE
		,@Message NVARCHAR(MAX)
		,@Row_Count INTEGER
		,@InputXML XML

	BEGIN TRY
		BEGIN TRANSACTION;

		SELECT @InputXML = CAST(x AS XML)
		
		FROM OPENROWSET(BULK '\\WHServer\\Users\leo.pickard\Desktop\Misc Bulk Inserts\Preorder Data\upload preorders-history.xml', SINGLE_BLOB) AS T(x)

		DROP TABLE

		IF EXISTS [dbo].[fPreorder History];
			CREATE TABLE [dbo].[fPreorder History] (
				[Preorder Code] NVARCHAR(70) NULL
				,[Season] NVARCHAR(20) NULL
				,[Type] INTEGER NULL
				,[Brand Code] NVARCHAR(3) NULL
				,[Category Code] NVARCHAR(50) NULL
				,[Item No] NVARCHAR(30) NULL
				,[Item Description] NVARCHAR(300) NULL
				,[Customer No] NVARCHAR(12) NULL
				,[Country Code] NVARCHAR(5) NULL
				,[Quantity] INTEGER NULL
				,[Value] DECIMAL(20, 8) NULL
				,[Currency] NVARCHAR(3) NULL
				,[Order Timestamp] DATETIME2(3) NULL
				,[Start Timestamp] DATETIME2(3) NULL
				,[End Timestamp] DATETIME2(3) NULL
				,[ETA Timestamp] DATETIME2(3) NULL
				);

		CREATE NONCLUSTERED INDEX IX_fPreorderHist_Preorder_Item ON [dbo].[fPreorder History] (
			[Preorder Code]
			,[Item No]
			) INCLUDE (
			[Customer No]
			,[Quantity]
			,[Value]
			,[Currency]
			,[Order Timestamp]
			,[Season]
			,[Brand Code]
			,[Category Code]
			,[Country Code]
			);

		CREATE NONCLUSTERED INDEX IX_fPreorderHist_Customer_Date ON [dbo].[fPreorder History] (
			[Customer No]
			,[Order Timestamp]
			) INCLUDE (
			[Preorder Code]
			,[Item No]
			,[Quantity]
			,[Value]
			,[Currency]
			,[Country Code]
			);

		INSERT INTO [fPreorder History] (
			[Preorder Code]
			,[Season]
			,[Type]
			,[Brand Code]
			,[Category Code]
			,[Item No]
			,[Item Description]
			,[Customer No]
			,[Country Code]
			,[Quantity]
			,[Value]
			,[Currency]
			,[Order Timestamp]
			,[Start Timestamp]
			,[End Timestamp]
			,[ETA Timestamp]
			)
		
		SELECT PreorderData.value('(preorder)[1]', 'NVARCHAR(70)')
			,PreorderData.value('(season)[1]', 'NVARCHAR(20)')
			,PreorderData.value('(type)[1]', 'INTEGER')
			,PreorderData.value('(brand)[1]', 'NVARCHAR(3)')
			,PreorderData.value('(dept)[1]', 'NVARCHAR(50)')
			,PreorderData.value('(part)[1]', 'NVARCHAR(30)')
			,PreorderData.value('(description)[1]', 'NVARCHAR(300)')
			,PreorderData.value('(customer)[1]', 'NVARCHAR(12)')
			,PreorderData.value('(country)[1]', 'NVARCHAR(5)')
			,PreorderData.value('(quantity)[1]', 'INTEGER')
			,CAST(REPLACE(PreorderData.value('(value)[1]', 'NVARCHAR(50)'), ',', '') AS DECIMAL(20, 8))
			,PreorderData.value('(currency)[1]', 'NVARCHAR(3)')
			,PreorderData.value('(datetime)[1]', 'DATETIME2(3)')
			,PreorderData.value('(start)[1]', 'DATETIME2(3)')
			,PreorderData.value('(end)[1]', 'DATETIME2(3)')
			,PreorderData.value('(eta)[1]', 'DATETIME2(3)')
		
		FROM @InputXML.nodes('data/row') AS X(PreorderData);

		ALTER INDEX ALL ON [dbo].[fPreorder History] REBUILD;

		UPDATE STATISTICS [dbo].[fPreorder History];

		SELECT @Timestamp = SYSDATETIME()
			,@Level = 'INFO'
			,@File_Name = 'Update fPreorder History.sql'
			,@Table = 'fPreorder History'
			,@Action = 'Update fPreorder History table with new data.'
			,@Row_Count = (
				SELECT COUNT(*)
				
				FROM [Warehouse].[dbo].[fPreorder History]
				)
			,@Start_Date = NULL
			,@End_Date = NULL
			,@Message = 'New data has been written to fPreorder History table from owtanet ftp csv.';

		EXEC [dbo].[Insert Record Into Database Log] @Timestamp = @Timestamp
			,@Level = @Level
			,@File_Name = @File_Name
			,@Table = @Table
			,@Action = @Action
			,@Row_Count = @Row_Count
			,@Start_Date = @Start_Date
			,@End_Date = @End_Date
			,@Message = @Message

		COMMIT TRANSACTION;
	
	END TRY

	BEGIN CATCH
		/*
        XACT_STATE() = 1: There is an active transaction that can still be committed or rolled back.
        XACT_STATE() = -1: The transaction is uncommittable and must be rolled back.
        XACT_STATE() = 0: There is no active transaction.
        */
		IF (XACT_STATE()) = - 1
		BEGIN
			SELECT @Timestamp = SYSDATETIME()
				,@Level = 'ERROR'
				,@File_Name = 'Update fPreorder History.sql'
				,@Table = 'fPreorder History'
				,@Action = 'Update fPreorder History table with new data.'
				,@Row_Count = NULL
				,@Start_Date = NULL
				,@End_Date = NULL
				,@Message = CAST((
						SELECT CONCAT (
								ERROR_NUMBER()
								,' - '
								,ERROR_MESSAGE()
								)
						) AS NVARCHAR(MAX))

			EXEC [dbo].[Insert Record Into Database Log] @Timestamp = @Timestamp
				,@Level = @Level
				,@File_Name = @File_Name
				,@Table = @Table
				,@Action = @Action
				,@Row_Count = @Row_Count
				,@Start_Date = @Start_Date
				,@End_Date = @End_Date
				,@Message = @Message;

			ROLLBACK TRANSACTION;
		
		END
		
		ELSE IF (XACT_STATE() = 1)
		BEGIN
			ROLLBACK TRANSACTION;
		
		END
	
	END CATCH

END

GO
