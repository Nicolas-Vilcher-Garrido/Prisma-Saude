-- Create database and schema if they don't already exist
IF DB_ID('SaudeDev') IS NULL
    CREATE DATABASE SaudeDev;
GO

USE SaudeDev;
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'app')
    EXEC('CREATE SCHEMA app');
GO

-- Drop table if it exists
IF OBJECT_ID('app.Atendimentos', 'U') IS NOT NULL
    DROP TABLE app.Atendimentos;
GO

-- Create the table
CREATE TABLE app.Atendimentos (
    Id INT IDENTITY(1,1) PRIMARY KEY,
    Data DATE,
    ClienteId INT,
    Operadora NVARCHAR(50),
    Procedimento NVARCHAR(50),
    Categoria NVARCHAR(10),
    Qtde INT,
    PrecoUnitario DECIMAL(18,2),
    Receita DECIMAL(18,2)
);
GO

-- Insert some fictitious records
INSERT INTO app.Atendimentos (Data, ClienteId, Operadora, Procedimento, Categoria, Qtde, PrecoUnitario, Receita) VALUES
    ('2024-01-05', 1, 'OperadoraA', 'Proc1', 'A', 2, 100.00, 200.00),
    ('2024-02-10', 2, 'OperadoraB', 'Proc2', 'B', 1, 300.00, 300.00),
    ('2024-03-15', 3, 'OperadoraC', 'Proc3', 'C', 5, 50.00, 250.00);
GO