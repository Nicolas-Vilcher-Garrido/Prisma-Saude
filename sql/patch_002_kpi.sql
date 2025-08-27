USE SaudeDev;
GO

-- 1) Tabela de log de cargas
IF OBJECT_ID('app.CargasLog') IS NULL
BEGIN
  CREATE TABLE app.CargasLog (
    Id               INT IDENTITY(1,1) PRIMARY KEY,
    Execucao         DATETIME2 NOT NULL DEFAULT SYSDATETIME(),
    LinhasLidas      INT       NOT NULL,
    LinhasPersistidas INT      NOT NULL,
    P90Receita       DECIMAL(18,2) NULL,
    Observacao       NVARCHAR(200) NULL
  );
END
GO

-- 2) View de receita mensal (apoia gráficos/validação)
IF OBJECT_ID('app.vw_ReceitaMensal') IS NOT NULL
  DROP VIEW app.vw_ReceitaMensal;
GO
CREATE VIEW app.vw_ReceitaMensal AS
SELECT
  CONVERT(date, DATEFROMPARTS(YEAR(Data), MONTH(Data), 1)) AS Mes,
  SUM(Receita) AS ReceitaTotal
FROM app.Atendimentos
GROUP BY DATEFROMPARTS(YEAR(Data), MONTH(Data), 1);
GO

-- 3) SP de KPIs (contagens + top 5)
IF OBJECT_ID('app.sp_KPIs') IS NOT NULL
  DROP PROCEDURE app.sp_KPIs;
GO
CREATE PROCEDURE app.sp_KPIs
  @DataIni date = NULL,
  @DataFim date = NULL
AS
BEGIN
  SET NOCOUNT ON;

  ;WITH F AS (
    SELECT *
    FROM app.Atendimentos
    WHERE (@DataIni IS NULL OR Data >= @DataIni)
      AND (@DataFim IS NULL OR Data <= @DataFim)
  )
  SELECT
    (SELECT COUNT(*) FROM F)     AS Linhas,
    (SELECT SUM(Receita) FROM F) AS ReceitaTotal,
    (SELECT MIN(Data) FROM F)    AS MinData,
    (SELECT MAX(Data) FROM F)    AS MaxData;

  -- Top 5 Operadoras
  SELECT TOP 5 Operadora, SUM(Receita) AS Receita
  FROM F
  GROUP BY Operadora
  ORDER BY Receita DESC;

  -- Top 5 Procedimentos
  SELECT TOP 5 Procedimento, SUM(Receita) AS Receita
  FROM F
  GROUP BY Procedimento
  ORDER BY Receita DESC;
END
GO
