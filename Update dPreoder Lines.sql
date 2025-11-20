USE [Warehouse]

GO

SET ANSI_NULLS ON

GO

SET QUOTED_IDENTIFIER ON

GO

CREATE
	OR

ALTER PROCEDURE [dbo].[Update dPreorder Lines]
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

	BEGIN TRY
		BEGIN TRANSACTION;

		DROP TABLE

		IF EXISTS [Warehouse].[dbo].[dPreorder Lines];
			CREATE TABLE [dPreorder Lines] (
				[Preorder Code] NVARCHAR(70) NULL
				,[Region] NVARCHAR(10) NULL
				,[Brand Code] NVARCHAR(3) NULL
				,[Type] INTEGER NULL
				,[Season] NVARCHAR(20) NULL
				,[Item No] NVARCHAR(30) NULL
				,[Item Description] NVARCHAR(300) NULL
				,[Price String] NVARCHAR(30) NULL
				,[Currency] NVARCHAR(3) NULL
				,[WHS] DECIMAL(20, 8) NULL
				,[SRP] DECIMAL(20, 8) NULL
				);

		BULK INSERT [dbo].[dPreorder Lines]
		
		FROM '\\WHServer\Users\leo.pickard\Desktop\Misc Bulk Inserts\Preorder Data\Active Import.csv' WITH (
				FIELDTERMINATOR = '~'
				,ROWTERMINATOR = '\n'
				,FIRSTROW = 2
				);

		ALTER TABLE [dbo].[dPreorder Lines] ADD [Row No] INT IDENTITY (
			1
			,1
			) NOT NULL;

		ALTER TABLE [dbo].[dPreorder Lines] ADD CONSTRAINT PK_dPreorder_Lines PRIMARY KEY ([Row No]);

		-- Most selective + common lookup: Preorder + Item
		CREATE NONCLUSTERED INDEX IX_dPreorderLines_Preorder_Item ON [dbo].[dPreorder Lines] (
			[Preorder Code]
			,[Item No]
			) INCLUDE (
			[Currency]
			,[WHS]
			,[SRP]
			,[Season]
			,[Brand Code]
			,[Region]
			,[Item Description]
			);

		CREATE NONCLUSTERED INDEX IX_dPreorderLines_Region_Season_Brand ON [dbo].[dPreorder Lines] (
			[Region]
			,[Season]
			,[Brand Code]
			) INCLUDE (
			[Preorder Code]
			,[Item No]
			,[WHS]
			,[SRP]
			);

		CREATE NONCLUSTERED INDEX IX_dPreorderLines_ItemNo ON [dbo].[dPreorder Lines] ([Item No]) INCLUDE (
			[Preorder Code]
			,[Item Description]
			,[Currency]
			,[WHS]
			,[SRP]
			);

		ALTER INDEX ALL ON [dbo].[dPreorder Lines] REBUILD;

		UPDATE STATISTICS [dbo].[dPreorder Lines];

		--The below code enters a success record into the update log table
		SELECT @Timestamp = SYSDATETIME()
			,@Level = 'INFO'
			,@File_Name = 'Update dPreorder Lines.sql'
			,@Table = 'dPreorder Lines'
			,@Action = 'Update dPreorder Lines table with new data.'
			,@Row_Count = (
				SELECT COUNT(*)
				
				FROM [Warehouse].[dbo].[dPreorder Lines]
				)
			,@Start_Date = NULL
			,@End_Date = NULL
			,@Message = 'New data has been written to dPreorder Lines table from owtanet ftp csv.';

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
				,@File_Name = 'Update dPreorder Lines.sql'
				,@Table = 'dPreorder Lines'
				,@Action = 'Update dPreorder Lines table with new data.'
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
