USE SaudeDev;
GO

INSERT INTO app.Atendimentos
(Data, ClienteId, Operadora, Procedimento, Categoria, Qtde, PrecoUnitario, Receita)
VALUES
('2024-04-10', 100, 'OperadoraD', 'ProcX', 'A', 10,  80.00,  800.00),
('2024-05-15', 101, 'OperadoraE', 'ProcY', 'B',  5, 200.00, 1000.00),
('2024-06-20', 102, 'OperadoraF', 'ProcZ', 'C',  8, 150.00, 1200.00),

('2025-01-10', 103, 'OperadoraA', 'Proc1', 'A',  6, 120.00,  720.00),
('2025-02-14', 104, 'OperadoraB', 'Proc2', 'B',  3, 350.00, 1050.00),
('2025-03-18', 105, 'OperadoraC', 'Proc3', 'C', 12,  60.00,  720.00);
GO
