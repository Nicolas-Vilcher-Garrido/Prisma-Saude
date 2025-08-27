/* =========================================================================
   Patch 001 – Hardening + Upsert
   - Adiciona LoadDate (rastreamento de carga)
   - Cria índices de performance
   - Cria chave natural única (dedupe)
   - Cria usuário dedicado (etl_user) com permissões mínimas
   - Cria proc de MERGE via staging (#tmp)
   ======================================================================== */

-- Garante DB (no seu init já cria; aqui só "assume")
USE SaudeDev;
GO

-- 1) Coluna de rastreio da carga (se não existir)
IF COL_LENGTH('app.Atendimentos','LoadDate') IS NULL
BEGIN
  ALTER TABLE app.Atendimentos
  ADD LoadDate DATETIME2 NOT NULL CONSTRAINT DF_At_LoadDate DEFAULT (SYSDATETIME());
END
GO

-- 2) Índices de performance (cria só se não existir)
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Atendimentos_Data' AND object_id = OBJECT_ID('app.Atendimentos'))
  CREATE INDEX IX_Atendimentos_Data ON app.Atendimentos (Data);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Atendimentos_Operadora' AND object_id = OBJECT_ID('app.Atendimentos'))
  CREATE INDEX IX_Atendimentos_Operadora ON app.Atendimentos (Operadora);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'IX_Atendimentos_Proc' AND object_id = OBJECT_ID('app.Atendimentos'))
  CREATE INDEX IX_Atendimentos_Proc ON app.Atendimentos (Procedimento);
GO

-- 3) Chave natural única para dedupe: (Data, ClienteId, Procedimento)
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'UX_Atendimentos_NaturalKey' AND object_id = OBJECT_ID('app.Atendimentos'))
  CREATE UNIQUE INDEX UX_Atendimentos_NaturalKey
  ON app.Atendimentos (Data, ClienteId, Procedimento);
GO

-- 4) Usuário dedicado para ETL (permissões mínimas)
IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = 'etl_user')
  CREATE LOGIN etl_user WITH PASSWORD = 'SenhaForte@2025';
GO

IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'etl_user')
  CREATE USER etl_user FOR LOGIN etl_user;
GO

-- Permissões de leitura/escrita básicas
EXEC sp_addrolemember 'db_datareader', 'etl_user';
EXEC sp_addrolemember 'db_datawriter', 'etl_user';
GO

-- 5) Proc de upsert (merge) a partir de #tmp_at
IF OBJECT_ID('app.sp_UpsertAtendimentos') IS NOT NULL
  DROP PROCEDURE app.sp_UpsertAtendimentos;
GO

CREATE PROCEDURE app.sp_UpsertAtendimentos
AS
BEGIN
  SET NOCOUNT ON;

  /* Espera-se uma tabela temporária #tmp_at com as colunas:
     Data date, ClienteId nvarchar(50), Operadora nvarchar(100),
     Procedimento nvarchar(100), Categoria nvarchar(10),
     Qtde int, PrecoUnitario decimal(18,2), Receita decimal(18,2)
  */

  MERGE app.Atendimentos AS t
  USING #tmp_at AS s
    ON  t.Data        = s.Data
    AND t.ClienteId   = s.ClienteId
    AND t.Procedimento= s.Procedimento
  WHEN MATCHED THEN
    UPDATE SET
      t.Operadora     = s.Operadora,
      t.Categoria     = s.Categoria,
      t.Qtde          = s.Qtde,
      t.PrecoUnitario = s.PrecoUnitario,
      t.Receita       = s.Receita,
      t.LoadDate      = SYSDATETIME()
  WHEN NOT MATCHED BY TARGET THEN
    INSERT (Data, ClienteId, Operadora, Procedimento, Categoria, Qtde, PrecoUnitario, Receita, LoadDate)
    VALUES (s.Data, s.ClienteId, s.Operadora, s.Procedimento, s.Categoria, s.Qtde, s.PrecoUnitario, s.Receita, SYSDATETIME());
END
GO
