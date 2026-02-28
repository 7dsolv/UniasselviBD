/*
Projeto: Faculda - Mapa do Tesouro de Dados
Uniasselvi: Adilson Oliveira
Engenharia de Software
Descricao: Base enterprise em camadas (core + log + telemetry + analytics)
Compatibilidade: SQL Server 2022+
Autor: Adilson Oliveira
Data: 2026-02-28
*/

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

IF DB_ID(N'Faculda') IS NULL
BEGIN
    PRINT N'Criando banco [Faculda]...';
    CREATE DATABASE [Faculda];
END;
GO

USE [Faculda];
GO

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

/* Limpeza de objetos legados (versoes anteriores do script) */
IF OBJECT_ID(N'financeiro.trg_Pagamento_Auditoria', N'TR') IS NOT NULL DROP TRIGGER financeiro.trg_Pagamento_Auditoria;
IF OBJECT_ID(N'academico.trg_Matricula_Auditoria', N'TR') IS NOT NULL DROP TRIGGER academico.trg_Matricula_Auditoria;
IF OBJECT_ID(N'academico.trg_Nota_ValidaLimite', N'TR') IS NOT NULL DROP TRIGGER academico.trg_Nota_ValidaLimite;
GO

IF OBJECT_ID(N'dbo.vw_VisaoUnicaFaculda', N'V') IS NOT NULL DROP VIEW dbo.vw_VisaoUnicaFaculda;
IF OBJECT_ID(N'academico.vw_RankingTurma', N'V') IS NOT NULL DROP VIEW academico.vw_RankingTurma;
IF OBJECT_ID(N'financeiro.vw_Inadimplencia', N'V') IS NOT NULL DROP VIEW financeiro.vw_Inadimplencia;
IF OBJECT_ID(N'academico.vw_PainelAluno', N'V') IS NOT NULL DROP VIEW academico.vw_PainelAluno;
GO

IF OBJECT_ID(N'financeiro.sp_RegistrarPagamento', N'P') IS NOT NULL DROP PROCEDURE financeiro.sp_RegistrarPagamento;
IF OBJECT_ID(N'academico.sp_LancarNotaFinal', N'P') IS NOT NULL DROP PROCEDURE academico.sp_LancarNotaFinal;
IF OBJECT_ID(N'academico.sp_MatricularAluno', N'P') IS NOT NULL DROP PROCEDURE academico.sp_MatricularAluno;
GO

IF OBJECT_ID(N'financeiro.fn_SaldoAluno', N'IF') IS NOT NULL DROP FUNCTION financeiro.fn_SaldoAluno;
IF OBJECT_ID(N'academico.fn_CRA_Aluno', N'FN') IS NOT NULL DROP FUNCTION academico.fn_CRA_Aluno;
GO

/* Schemas */
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'core') EXEC (N'CREATE SCHEMA core AUTHORIZATION dbo;');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'log') EXEC (N'CREATE SCHEMA log AUTHORIZATION dbo;');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'telemetry') EXEC (N'CREATE SCHEMA telemetry AUTHORIZATION dbo;');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'analytics') EXEC (N'CREATE SCHEMA analytics AUTHORIZATION dbo;');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'security') EXEC (N'CREATE SCHEMA security AUTHORIZATION dbo;');
GO

/* Drop em ordem de dependencia */
IF OBJECT_ID(N'analytics.FatoTelemetria', N'U') IS NOT NULL DROP TABLE analytics.FatoTelemetria;
IF OBJECT_ID(N'analytics.FatoFinanceiro', N'U') IS NOT NULL DROP TABLE analytics.FatoFinanceiro;
IF OBJECT_ID(N'analytics.FatoAcademico', N'U') IS NOT NULL DROP TABLE analytics.FatoAcademico;
IF OBJECT_ID(N'analytics.KPI_Diario', N'U') IS NOT NULL DROP TABLE analytics.KPI_Diario;
IF OBJECT_ID(N'analytics.DimTurma', N'U') IS NOT NULL DROP TABLE analytics.DimTurma;
IF OBJECT_ID(N'analytics.DimAluno', N'U') IS NOT NULL DROP TABLE analytics.DimAluno;
IF OBJECT_ID(N'analytics.DimCurso', N'U') IS NOT NULL DROP TABLE analytics.DimCurso;
IF OBJECT_ID(N'analytics.DimTempo', N'U') IS NOT NULL DROP TABLE analytics.DimTempo;

IF OBJECT_ID(N'telemetry.MetricMinute', N'U') IS NOT NULL DROP TABLE telemetry.MetricMinute;
IF OBJECT_ID(N'telemetry.EventStream', N'U') IS NOT NULL DROP TABLE telemetry.EventStream;
IF OBJECT_ID(N'telemetry.SessionApp', N'U') IS NOT NULL DROP TABLE telemetry.SessionApp;
IF OBJECT_ID(N'telemetry.EventType', N'U') IS NOT NULL DROP TABLE telemetry.EventType;

IF OBJECT_ID(N'log.AccessLog', N'U') IS NOT NULL DROP TABLE log.AccessLog;
IF OBJECT_ID(N'log.ErrorLog', N'U') IS NOT NULL DROP TABLE log.ErrorLog;
IF OBJECT_ID(N'log.ProcessRun', N'U') IS NOT NULL DROP TABLE log.ProcessRun;
IF OBJECT_ID(N'log.ChangeAudit', N'U') IS NOT NULL DROP TABLE log.ChangeAudit;

IF OBJECT_ID(N'core.Pagamento', N'U') IS NOT NULL DROP TABLE core.Pagamento;
IF OBJECT_ID(N'core.ParcelaFinanceira', N'U') IS NOT NULL DROP TABLE core.ParcelaFinanceira;
IF OBJECT_ID(N'core.PlanoFinanceiro', N'U') IS NOT NULL DROP TABLE core.PlanoFinanceiro;
IF OBJECT_ID(N'core.Nota', N'U') IS NOT NULL DROP TABLE core.Nota;
IF OBJECT_ID(N'core.Avaliacao', N'U') IS NOT NULL DROP TABLE core.Avaliacao;
IF OBJECT_ID(N'core.Matricula', N'U') IS NOT NULL DROP TABLE core.Matricula;
IF OBJECT_ID(N'core.Turma', N'U') IS NOT NULL DROP TABLE core.Turma;
IF OBJECT_ID(N'core.Disciplina', N'U') IS NOT NULL DROP TABLE core.Disciplina;
IF OBJECT_ID(N'core.Curso', N'U') IS NOT NULL DROP TABLE core.Curso;
IF OBJECT_ID(N'core.Professor', N'U') IS NOT NULL DROP TABLE core.Professor;
IF OBJECT_ID(N'core.Aluno', N'U') IS NOT NULL DROP TABLE core.Aluno;
IF OBJECT_ID(N'core.Departamento', N'U') IS NOT NULL DROP TABLE core.Departamento;
IF OBJECT_ID(N'core.Pessoa', N'U') IS NOT NULL DROP TABLE core.Pessoa;

IF OBJECT_ID(N'security.UsuarioSistema', N'U') IS NOT NULL DROP TABLE security.UsuarioSistema;
GO

/* =============== CORE LAYER =============== */
CREATE TABLE core.Pessoa
(
    PessoaID INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_core_Pessoa PRIMARY KEY,
    PessoaGuid UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_core_Pessoa_Guid DEFAULT NEWID(),
    NomeExibicao NVARCHAR(140) NOT NULL,
    DocumentoHash VARBINARY(32) NULL,
    Email NVARCHAR(180) NOT NULL,
    Telefone VARCHAR(20) NULL,
    DataNascimento DATE NULL,
    Ativo BIT NOT NULL CONSTRAINT DF_core_Pessoa_Ativo DEFAULT (1),
    CriadoEm DATETIME2(0) NOT NULL CONSTRAINT DF_core_Pessoa_CriadoEm DEFAULT SYSDATETIME(),
    AtualizadoEm DATETIME2(0) NOT NULL CONSTRAINT DF_core_Pessoa_AtualizadoEm DEFAULT SYSDATETIME(),
    CONSTRAINT UQ_core_Pessoa_Guid UNIQUE (PessoaGuid),
    CONSTRAINT UQ_core_Pessoa_Email UNIQUE (Email)
);
GO

CREATE TABLE core.Departamento
(
    DepartamentoID INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_core_Departamento PRIMARY KEY,
    Nome NVARCHAR(120) NOT NULL,
    Sigla VARCHAR(12) NOT NULL,
    CentroCusto VARCHAR(20) NULL,
    EmailContato NVARCHAR(180) NULL,
    Ativo BIT NOT NULL CONSTRAINT DF_core_Departamento_Ativo DEFAULT (1),
    CriadoEm DATETIME2(0) NOT NULL CONSTRAINT DF_core_Departamento_CriadoEm DEFAULT SYSDATETIME(),
    CONSTRAINT UQ_core_Departamento_Nome UNIQUE (Nome),
    CONSTRAINT UQ_core_Departamento_Sigla UNIQUE (Sigla)
);
GO

CREATE TABLE core.Aluno
(
    AlunoID INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_core_Aluno PRIMARY KEY,
    PessoaID INT NOT NULL,
    RA VARCHAR(20) NOT NULL,
    DataIngresso DATE NOT NULL CONSTRAINT DF_core_Aluno_DataIngresso DEFAULT CONVERT(DATE, GETDATE()),
    StatusAcademico VARCHAR(20) NOT NULL CONSTRAINT DF_core_Aluno_Status DEFAULT ('ATIVO'),
    Bolsista BIT NOT NULL CONSTRAINT DF_core_Aluno_Bolsista DEFAULT (0),
    PercentualBolsa DECIMAL(5,2) NOT NULL CONSTRAINT DF_core_Aluno_PercentualBolsa DEFAULT (0),
    CriadoEm DATETIME2(0) NOT NULL CONSTRAINT DF_core_Aluno_CriadoEm DEFAULT SYSDATETIME(),
    CONSTRAINT UQ_core_Aluno_Pessoa UNIQUE (PessoaID),
    CONSTRAINT UQ_core_Aluno_RA UNIQUE (RA),
    CONSTRAINT FK_core_Aluno_Pessoa FOREIGN KEY (PessoaID) REFERENCES core.Pessoa (PessoaID),
    CONSTRAINT CK_core_Aluno_Status CHECK (StatusAcademico IN ('ATIVO', 'TRANCADO', 'FORMADO', 'DESLIGADO')),
    CONSTRAINT CK_core_Aluno_Bolsa CHECK
    (
        (Bolsista = 0 AND PercentualBolsa = 0)
        OR
        (Bolsista = 1 AND PercentualBolsa > 0 AND PercentualBolsa <= 100)
    )
);
GO

CREATE TABLE core.Professor
(
    ProfessorID INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_core_Professor PRIMARY KEY,
    PessoaID INT NOT NULL,
    DepartamentoID INT NOT NULL,
    Titulacao VARCHAR(20) NOT NULL,
    RegimeTrabalho VARCHAR(20) NOT NULL,
    DataAdmissao DATE NOT NULL,
    Ativo BIT NOT NULL CONSTRAINT DF_core_Professor_Ativo DEFAULT (1),
    CriadoEm DATETIME2(0) NOT NULL CONSTRAINT DF_core_Professor_CriadoEm DEFAULT SYSDATETIME(),
    CONSTRAINT UQ_core_Professor_Pessoa UNIQUE (PessoaID),
    CONSTRAINT FK_core_Professor_Pessoa FOREIGN KEY (PessoaID) REFERENCES core.Pessoa (PessoaID),
    CONSTRAINT FK_core_Professor_Departamento FOREIGN KEY (DepartamentoID) REFERENCES core.Departamento (DepartamentoID),
    CONSTRAINT CK_core_Professor_Titulacao CHECK (Titulacao IN ('GRADUACAO', 'ESPECIALIZACAO', 'MESTRADO', 'DOUTORADO', 'POS_DOUTORADO')),
    CONSTRAINT CK_core_Professor_Regime CHECK (RegimeTrabalho IN ('HORISTA', 'PARCIAL', 'INTEGRAL'))
);
GO

CREATE TABLE core.Curso
(
    CursoID INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_core_Curso PRIMARY KEY,
    DepartamentoID INT NOT NULL,
    Codigo VARCHAR(16) NOT NULL,
    Nome NVARCHAR(120) NOT NULL,
    Nivel VARCHAR(20) NOT NULL,
    Modalidade VARCHAR(20) NOT NULL,
    CargaHorariaTotal SMALLINT NOT NULL,
    SemestresPrevistos TINYINT NOT NULL,
    Ativo BIT NOT NULL CONSTRAINT DF_core_Curso_Ativo DEFAULT (1),
    CriadoEm DATETIME2(0) NOT NULL CONSTRAINT DF_core_Curso_CriadoEm DEFAULT SYSDATETIME(),
    CONSTRAINT UQ_core_Curso_Codigo UNIQUE (Codigo),
    CONSTRAINT UQ_core_Curso_Nome UNIQUE (Nome),
    CONSTRAINT FK_core_Curso_Departamento FOREIGN KEY (DepartamentoID) REFERENCES core.Departamento (DepartamentoID),
    CONSTRAINT CK_core_Curso_Nivel CHECK (Nivel IN ('GRADUACAO', 'POS', 'TECNICO', 'LIVRE')),
    CONSTRAINT CK_core_Curso_Modalidade CHECK (Modalidade IN ('PRESENCIAL', 'EAD', 'HIBRIDO')),
    CONSTRAINT CK_core_Curso_Carga CHECK (CargaHorariaTotal BETWEEN 120 AND 12000),
    CONSTRAINT CK_core_Curso_Semestres CHECK (SemestresPrevistos BETWEEN 1 AND 20)
);
GO

CREATE TABLE core.Disciplina
(
    DisciplinaID INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_core_Disciplina PRIMARY KEY,
    CursoID INT NOT NULL,
    Codigo VARCHAR(20) NOT NULL,
    Nome NVARCHAR(120) NOT NULL,
    CargaHoraria SMALLINT NOT NULL,
    Creditos TINYINT NOT NULL,
    SemestreSugerido TINYINT NOT NULL,
    Ementa NVARCHAR(1000) NULL,
    Ativa BIT NOT NULL CONSTRAINT DF_core_Disciplina_Ativa DEFAULT (1),
    CriadoEm DATETIME2(0) NOT NULL CONSTRAINT DF_core_Disciplina_CriadoEm DEFAULT SYSDATETIME(),
    CONSTRAINT UQ_core_Disciplina_Codigo UNIQUE (Codigo),
    CONSTRAINT UQ_core_Disciplina_CursoNome UNIQUE (CursoID, Nome),
    CONSTRAINT FK_core_Disciplina_Curso FOREIGN KEY (CursoID) REFERENCES core.Curso (CursoID),
    CONSTRAINT CK_core_Disciplina_CH CHECK (CargaHoraria BETWEEN 15 AND 400),
    CONSTRAINT CK_core_Disciplina_Creditos CHECK (Creditos BETWEEN 1 AND 20),
    CONSTRAINT CK_core_Disciplina_Semestre CHECK (SemestreSugerido BETWEEN 1 AND 20)
);
GO

CREATE TABLE core.Turma
(
    TurmaID INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_core_Turma PRIMARY KEY,
    DisciplinaID INT NOT NULL,
    ProfessorID INT NOT NULL,
    CodigoTurma VARCHAR(24) NOT NULL,
    Ano SMALLINT NOT NULL,
    Periodo TINYINT NOT NULL,
    Turno VARCHAR(10) NOT NULL,
    Vagas SMALLINT NOT NULL,
    DataInicio DATE NOT NULL,
    DataFim DATE NOT NULL,
    Encerrada BIT NOT NULL CONSTRAINT DF_core_Turma_Encerrada DEFAULT (0),
    CriadoEm DATETIME2(0) NOT NULL CONSTRAINT DF_core_Turma_CriadoEm DEFAULT SYSDATETIME(),
    CONSTRAINT UQ_core_Turma_Codigo UNIQUE (CodigoTurma),
    CONSTRAINT FK_core_Turma_Disciplina FOREIGN KEY (DisciplinaID) REFERENCES core.Disciplina (DisciplinaID),
    CONSTRAINT FK_core_Turma_Professor FOREIGN KEY (ProfessorID) REFERENCES core.Professor (ProfessorID),
    CONSTRAINT CK_core_Turma_Ano CHECK (Ano BETWEEN 2000 AND 2100),
    CONSTRAINT CK_core_Turma_Periodo CHECK (Periodo IN (1,2,3)),
    CONSTRAINT CK_core_Turma_Turno CHECK (Turno IN ('MANHA', 'TARDE', 'NOITE', 'EAD')),
    CONSTRAINT CK_core_Turma_Vagas CHECK (Vagas BETWEEN 1 AND 1000),
    CONSTRAINT CK_core_Turma_Datas CHECK (DataFim > DataInicio)
);
GO

CREATE TABLE core.Matricula
(
    MatriculaID BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_core_Matricula PRIMARY KEY,
    AlunoID INT NOT NULL,
    TurmaID INT NOT NULL,
    DataMatricula DATETIME2(0) NOT NULL CONSTRAINT DF_core_Matricula_Data DEFAULT SYSDATETIME(),
    OrigemMatricula VARCHAR(20) NOT NULL CONSTRAINT DF_core_Matricula_Origem DEFAULT ('PORTAL'),
    StatusMatricula VARCHAR(20) NOT NULL CONSTRAINT DF_core_Matricula_Status DEFAULT ('ATIVA'),
    FrequenciaPercentual DECIMAL(5,2) NOT NULL CONSTRAINT DF_core_Matricula_Frequencia DEFAULT (0),
    NotaFinal DECIMAL(5,2) NULL,
    UltimaAtualizacao DATETIME2(0) NOT NULL CONSTRAINT DF_core_Matricula_UltimaAtualizacao DEFAULT SYSDATETIME(),
    CONSTRAINT UQ_core_Matricula_AlunoTurma UNIQUE (AlunoID, TurmaID),
    CONSTRAINT FK_core_Matricula_Aluno FOREIGN KEY (AlunoID) REFERENCES core.Aluno (AlunoID),
    CONSTRAINT FK_core_Matricula_Turma FOREIGN KEY (TurmaID) REFERENCES core.Turma (TurmaID),
    CONSTRAINT CK_core_Matricula_Origem CHECK (OrigemMatricula IN ('PORTAL', 'SECRETARIA', 'API', 'BATCH')),
    CONSTRAINT CK_core_Matricula_Status CHECK (StatusMatricula IN ('ATIVA', 'CANCELADA', 'TRANCADA', 'APROVADA', 'REPROVADA')),
    CONSTRAINT CK_core_Matricula_Frequencia CHECK (FrequenciaPercentual BETWEEN 0 AND 100),
    CONSTRAINT CK_core_Matricula_Nota CHECK (NotaFinal IS NULL OR NotaFinal BETWEEN 0 AND 10)
);
GO

CREATE TABLE core.Avaliacao
(
    AvaliacaoID INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_core_Avaliacao PRIMARY KEY,
    TurmaID INT NOT NULL,
    NomeAvaliacao NVARCHAR(80) NOT NULL,
    TipoAvaliacao VARCHAR(20) NOT NULL,
    Peso DECIMAL(5,2) NOT NULL,
    NotaMaxima DECIMAL(5,2) NOT NULL,
    DataAplicacao DATE NOT NULL,
    CriadoEm DATETIME2(0) NOT NULL CONSTRAINT DF_core_Avaliacao_CriadoEm DEFAULT SYSDATETIME(),
    CONSTRAINT UQ_core_Avaliacao_TurmaNome UNIQUE (TurmaID, NomeAvaliacao),
    CONSTRAINT FK_core_Avaliacao_Turma FOREIGN KEY (TurmaID) REFERENCES core.Turma (TurmaID),
    CONSTRAINT CK_core_Avaliacao_Tipo CHECK (TipoAvaliacao IN ('PROVA', 'TRABALHO', 'PROJETO', 'QUIZ', 'RECUPERACAO')),
    CONSTRAINT CK_core_Avaliacao_Peso CHECK (Peso > 0 AND Peso <= 100),
    CONSTRAINT CK_core_Avaliacao_NotaMax CHECK (NotaMaxima > 0 AND NotaMaxima <= 100)
);
GO

CREATE TABLE core.Nota
(
    NotaID BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_core_Nota PRIMARY KEY,
    AvaliacaoID INT NOT NULL,
    MatriculaID BIGINT NOT NULL,
    NotaObtida DECIMAL(5,2) NOT NULL,
    Comentario NVARCHAR(250) NULL,
    LancadaEm DATETIME2(0) NOT NULL CONSTRAINT DF_core_Nota_LancadaEm DEFAULT SYSDATETIME(),
    CONSTRAINT UQ_core_Nota_AvaliacaoMatricula UNIQUE (AvaliacaoID, MatriculaID),
    CONSTRAINT FK_core_Nota_Avaliacao FOREIGN KEY (AvaliacaoID) REFERENCES core.Avaliacao (AvaliacaoID),
    CONSTRAINT FK_core_Nota_Matricula FOREIGN KEY (MatriculaID) REFERENCES core.Matricula (MatriculaID),
    CONSTRAINT CK_core_Nota_Valor CHECK (NotaObtida >= 0)
);
GO

CREATE TABLE core.PlanoFinanceiro
(
    PlanoFinanceiroID INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_core_PlanoFinanceiro PRIMARY KEY,
    CursoID INT NOT NULL,
    AnoReferencia SMALLINT NOT NULL,
    ValorMensalBase DECIMAL(10,2) NOT NULL,
    DiaVencimento TINYINT NOT NULL,
    PercentualMulta DECIMAL(5,2) NOT NULL CONSTRAINT DF_core_Plano_Multa DEFAULT (2),
    PercentualJurosMes DECIMAL(5,2) NOT NULL CONSTRAINT DF_core_Plano_Juros DEFAULT (1),
    Ativo BIT NOT NULL CONSTRAINT DF_core_Plano_Ativo DEFAULT (1),
    CONSTRAINT UQ_core_Plano_CursoAno UNIQUE (CursoID, AnoReferencia),
    CONSTRAINT FK_core_Plano_Curso FOREIGN KEY (CursoID) REFERENCES core.Curso (CursoID),
    CONSTRAINT CK_core_Plano_Ano CHECK (AnoReferencia BETWEEN 2000 AND 2100),
    CONSTRAINT CK_core_Plano_Valor CHECK (ValorMensalBase > 0),
    CONSTRAINT CK_core_Plano_Dia CHECK (DiaVencimento BETWEEN 1 AND 28),
    CONSTRAINT CK_core_Plano_Pct CHECK (PercentualMulta BETWEEN 0 AND 100 AND PercentualJurosMes BETWEEN 0 AND 100)
);
GO

CREATE TABLE core.ParcelaFinanceira
(
    ParcelaID BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_core_ParcelaFinanceira PRIMARY KEY,
    AlunoID INT NOT NULL,
    PlanoFinanceiroID INT NULL,
    Competencia CHAR(7) NOT NULL,
    DataVencimento DATE NOT NULL,
    ValorOriginal DECIMAL(10,2) NOT NULL,
    ValorDesconto DECIMAL(10,2) NOT NULL CONSTRAINT DF_core_Parcela_Desconto DEFAULT (0),
    ValorMulta DECIMAL(10,2) NOT NULL CONSTRAINT DF_core_Parcela_Multa DEFAULT (0),
    ValorLiquido AS (ValorOriginal - ValorDesconto + ValorMulta) PERSISTED,
    ValorPago DECIMAL(10,2) NULL,
    DataPagamento DATE NULL,
    StatusParcela VARCHAR(20) NOT NULL CONSTRAINT DF_core_Parcela_Status DEFAULT ('ABERTA'),
    CanalGeracao VARCHAR(20) NOT NULL CONSTRAINT DF_core_Parcela_Canal DEFAULT ('MENSALIDADE'),
    Observacao NVARCHAR(250) NULL,
    CONSTRAINT UQ_core_Parcela_AlunoCompetencia UNIQUE (AlunoID, Competencia),
    CONSTRAINT FK_core_Parcela_Aluno FOREIGN KEY (AlunoID) REFERENCES core.Aluno (AlunoID),
    CONSTRAINT FK_core_Parcela_Plano FOREIGN KEY (PlanoFinanceiroID) REFERENCES core.PlanoFinanceiro (PlanoFinanceiroID),
    CONSTRAINT CK_core_Parcela_Competencia CHECK
    (
        Competencia LIKE '[1-2][0-9][0-9][0-9]-[0-1][0-9]'
        AND SUBSTRING(Competencia, 6, 2) BETWEEN '01' AND '12'
    ),
    CONSTRAINT CK_core_Parcela_Valores CHECK
    (
        ValorOriginal > 0
        AND ValorDesconto >= 0
        AND ValorMulta >= 0
        AND ValorDesconto <= ValorOriginal
        AND (ValorPago IS NULL OR ValorPago >= 0)
    ),
    CONSTRAINT CK_core_Parcela_Status CHECK (StatusParcela IN ('ABERTA', 'PAGA', 'ATRASADA', 'NEGOCIADA', 'CANCELADA')),
    CONSTRAINT CK_core_Parcela_Canal CHECK (CanalGeracao IN ('MENSALIDADE', 'ACORDO', 'SERVICO', 'MULTA', 'OUTRO'))
);
GO

CREATE TABLE core.Pagamento
(
    PagamentoID BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_core_Pagamento PRIMARY KEY,
    ParcelaID BIGINT NOT NULL,
    DataPagamento DATETIME2(0) NOT NULL CONSTRAINT DF_core_Pagamento_Data DEFAULT SYSDATETIME(),
    ValorPagamento DECIMAL(10,2) NOT NULL,
    MeioPagamento VARCHAR(20) NOT NULL,
    GatewayStatus VARCHAR(20) NOT NULL CONSTRAINT DF_core_Pagamento_GatewayStatus DEFAULT ('CONFIRMED'),
    ReferenciaExterna VARCHAR(80) NULL,
    UsuarioLancamento NVARCHAR(80) NULL,
    Observacao NVARCHAR(250) NULL,
    CONSTRAINT FK_core_Pagamento_Parcela FOREIGN KEY (ParcelaID) REFERENCES core.ParcelaFinanceira (ParcelaID) ON DELETE CASCADE,
    CONSTRAINT CK_core_Pagamento_Valor CHECK (ValorPagamento > 0),
    CONSTRAINT CK_core_Pagamento_Meio CHECK (MeioPagamento IN ('PIX', 'CARTAO', 'BOLETO', 'TRANSFERENCIA', 'DINHEIRO')),
    CONSTRAINT CK_core_Pagamento_Gateway CHECK (GatewayStatus IN ('PENDING', 'CONFIRMED', 'FAILED', 'CHARGEBACK'))
);
GO

/* =============== SECURITY LAYER =============== */
CREATE TABLE security.UsuarioSistema
(
    UsuarioID INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_security_UsuarioSistema PRIMARY KEY,
    Login VARCHAR(50) NOT NULL,
    NomeExibicao NVARCHAR(100) NOT NULL,
    Email NVARCHAR(180) NOT NULL,
    Perfil VARCHAR(20) NOT NULL,
    Ativo BIT NOT NULL CONSTRAINT DF_security_Usuario_Ativo DEFAULT (1),
    CriadoEm DATETIME2(0) NOT NULL CONSTRAINT DF_security_Usuario_CriadoEm DEFAULT SYSDATETIME(),
    CONSTRAINT UQ_security_Usuario_Login UNIQUE (Login),
    CONSTRAINT UQ_security_Usuario_Email UNIQUE (Email),
    CONSTRAINT CK_security_Usuario_Perfil CHECK (Perfil IN ('ADMIN', 'CORE_OPS', 'FINANCE_OPS', 'ANALYTICS', 'OBSERVER'))
);
GO

/* =============== LOG LAYER =============== */
CREATE TABLE log.ChangeAudit
(
    AuditID BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_log_ChangeAudit PRIMARY KEY,
    OrigemSchema SYSNAME NOT NULL,
    OrigemObjeto SYSNAME NOT NULL,
    ChaveRegistro NVARCHAR(200) NULL,
    Operacao VARCHAR(10) NOT NULL,
    DadosAntes NVARCHAR(MAX) NULL,
    DadosDepois NVARCHAR(MAX) NULL,
    AppUser NVARCHAR(128) NOT NULL CONSTRAINT DF_log_ChangeAudit_AppUser DEFAULT SUSER_SNAME(),
    HostName NVARCHAR(128) NOT NULL CONSTRAINT DF_log_ChangeAudit_Host DEFAULT HOST_NAME(),
    EventTime DATETIME2(0) NOT NULL CONSTRAINT DF_log_ChangeAudit_EventTime DEFAULT SYSDATETIME(),
    CorrelationID UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_log_ChangeAudit_Corr DEFAULT NEWID(),
    CONSTRAINT CK_log_ChangeAudit_Operacao CHECK (Operacao IN ('INSERT', 'UPDATE', 'DELETE'))
);
GO

CREATE TABLE log.ProcessRun
(
    ProcessRunID BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_log_ProcessRun PRIMARY KEY,
    ProcessoNome VARCHAR(120) NOT NULL,
    Camada VARCHAR(20) NOT NULL,
    IniciadoEm DATETIME2(0) NOT NULL CONSTRAINT DF_log_ProcessRun_Inicio DEFAULT SYSDATETIME(),
    FinalizadoEm DATETIME2(0) NULL,
    StatusExecucao VARCHAR(20) NOT NULL CONSTRAINT DF_log_ProcessRun_Status DEFAULT ('STARTED'),
    LinhasLidas INT NOT NULL CONSTRAINT DF_log_ProcessRun_Lidas DEFAULT (0),
    LinhasProcessadas INT NOT NULL CONSTRAINT DF_log_ProcessRun_Processadas DEFAULT (0),
    LinhasRejeitadas INT NOT NULL CONSTRAINT DF_log_ProcessRun_Rejeitadas DEFAULT (0),
    Mensagem NVARCHAR(2000) NULL,
    CorrelationID UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_log_ProcessRun_Corr DEFAULT NEWID(),
    CONSTRAINT CK_log_ProcessRun_Camada CHECK (Camada IN ('CORE', 'LOG', 'TELEMETRY', 'ANALYTICS')),
    CONSTRAINT CK_log_ProcessRun_Status CHECK (StatusExecucao IN ('STARTED', 'SUCCESS', 'FAILED', 'PARTIAL'))
);
GO

CREATE TABLE log.ErrorLog
(
    ErrorID BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_log_ErrorLog PRIMARY KEY,
    ProcessoNome VARCHAR(120) NOT NULL,
    Etapa VARCHAR(120) NULL,
    NumeroErro INT NULL,
    Severidade SMALLINT NULL,
    Estado SMALLINT NULL,
    ProcedureName SYSNAME NULL,
    Linha INT NULL,
    Mensagem NVARCHAR(MAX) NOT NULL,
    Payload NVARCHAR(MAX) NULL,
    CorrelationID UNIQUEIDENTIFIER NULL,
    CriadoEm DATETIME2(0) NOT NULL CONSTRAINT DF_log_ErrorLog_CriadoEm DEFAULT SYSDATETIME()
);
GO

CREATE TABLE log.AccessLog
(
    AccessLogID BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_log_AccessLog PRIMARY KEY,
    UsuarioID INT NULL,
    LoginEfetivo VARCHAR(50) NOT NULL,
    Acao VARCHAR(80) NOT NULL,
    Recurso VARCHAR(200) NOT NULL,
    IpOrigem VARCHAR(45) NULL,
    UserAgent NVARCHAR(400) NULL,
    StatusCode SMALLINT NOT NULL,
    DuracaoMs INT NULL,
    CriadoEm DATETIME2(0) NOT NULL CONSTRAINT DF_log_AccessLog_CriadoEm DEFAULT SYSDATETIME(),
    CONSTRAINT FK_log_AccessLog_Usuario FOREIGN KEY (UsuarioID) REFERENCES security.UsuarioSistema (UsuarioID)
);
GO
/* =============== TELEMETRY LAYER =============== */
CREATE TABLE telemetry.EventType
(
    EventTypeID INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_telemetry_EventType PRIMARY KEY,
    EventCode VARCHAR(80) NOT NULL,
    Categoria VARCHAR(40) NOT NULL,
    Severidade TINYINT NOT NULL CONSTRAINT DF_telemetry_EventType_Severidade DEFAULT (1),
    Ativo BIT NOT NULL CONSTRAINT DF_telemetry_EventType_Ativo DEFAULT (1),
    CriadoEm DATETIME2(0) NOT NULL CONSTRAINT DF_telemetry_EventType_CriadoEm DEFAULT SYSDATETIME(),
    CONSTRAINT UQ_telemetry_EventType_Code UNIQUE (EventCode),
    CONSTRAINT CK_telemetry_EventType_Severidade CHECK (Severidade BETWEEN 0 AND 5)
);
GO

CREATE TABLE telemetry.SessionApp
(
    SessionID BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_telemetry_SessionApp PRIMARY KEY,
    AlunoID INT NULL,
    UsuarioID INT NULL,
    ClientID VARCHAR(80) NOT NULL,
    Platform VARCHAR(20) NOT NULL,
    AppVersion VARCHAR(30) NULL,
    StartedAt DATETIME2(0) NOT NULL CONSTRAINT DF_telemetry_SessionApp_StartedAt DEFAULT SYSDATETIME(),
    EndedAt DATETIME2(0) NULL,
    IsAuthenticated BIT NOT NULL CONSTRAINT DF_telemetry_SessionApp_IsAuth DEFAULT (0),
    IpAddress VARCHAR(45) NULL,
    CountryCode CHAR(2) NULL,
    CONSTRAINT FK_telemetry_SessionApp_Aluno FOREIGN KEY (AlunoID) REFERENCES core.Aluno (AlunoID),
    CONSTRAINT FK_telemetry_SessionApp_Usuario FOREIGN KEY (UsuarioID) REFERENCES security.UsuarioSistema (UsuarioID),
    CONSTRAINT CK_telemetry_SessionApp_Platform CHECK (Platform IN ('WEB', 'MOBILE', 'DESKTOP', 'API', 'BATCH'))
);
GO

CREATE TABLE telemetry.EventStream
(
    EventID BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_telemetry_EventStream PRIMARY KEY,
    EventTypeID INT NOT NULL,
    SessionID BIGINT NULL,
    AlunoID INT NULL,
    OccurredAt DATETIME2(0) NOT NULL CONSTRAINT DF_telemetry_EventStream_OccurredAt DEFAULT SYSDATETIME(),
    IngestedAt DATETIME2(0) NOT NULL CONSTRAINT DF_telemetry_EventStream_IngestedAt DEFAULT SYSDATETIME(),
    SourceSystem VARCHAR(40) NOT NULL,
    CorrelationID UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_telemetry_EventStream_Corr DEFAULT NEWID(),
    EventKey VARCHAR(100) NULL,
    DurationMs INT NULL,
    NumericValue DECIMAL(18,4) NULL,
    BooleanValue BIT NULL,
    PayloadJson NVARCHAR(MAX) NULL,
    ProcessingStatus CHAR(1) NOT NULL CONSTRAINT DF_telemetry_EventStream_Status DEFAULT ('N'),
    CONSTRAINT FK_telemetry_EventStream_EventType FOREIGN KEY (EventTypeID) REFERENCES telemetry.EventType (EventTypeID),
    CONSTRAINT FK_telemetry_EventStream_Session FOREIGN KEY (SessionID) REFERENCES telemetry.SessionApp (SessionID),
    CONSTRAINT FK_telemetry_EventStream_Aluno FOREIGN KEY (AlunoID) REFERENCES core.Aluno (AlunoID),
    CONSTRAINT CK_telemetry_EventStream_Duration CHECK (DurationMs IS NULL OR DurationMs >= 0),
    CONSTRAINT CK_telemetry_EventStream_Status CHECK (ProcessingStatus IN ('N', 'P', 'E', 'D')),
    CONSTRAINT CK_telemetry_EventStream_Json CHECK (PayloadJson IS NULL OR ISJSON(PayloadJson) = 1)
);
GO

CREATE TABLE telemetry.MetricMinute
(
    MetricMinuteID BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_telemetry_MetricMinute PRIMARY KEY,
    MetricName VARCHAR(100) NOT NULL,
    MetricMinute DATETIME2(0) NOT NULL,
    Dimension1 VARCHAR(80) NULL,
    Dimension2 VARCHAR(80) NULL,
    MetricValue DECIMAL(18,4) NOT NULL,
    CreatedAt DATETIME2(0) NOT NULL CONSTRAINT DF_telemetry_MetricMinute_CreatedAt DEFAULT SYSDATETIME(),
    CONSTRAINT UQ_telemetry_MetricMinute UNIQUE (MetricName, MetricMinute, Dimension1, Dimension2)
);
GO

/* =============== ANALYTICS LAYER =============== */
CREATE TABLE analytics.DimTempo
(
    TempoKey INT NOT NULL CONSTRAINT PK_analytics_DimTempo PRIMARY KEY,
    DataCompleta DATE NOT NULL,
    Ano SMALLINT NOT NULL,
    Mes TINYINT NOT NULL,
    Dia TINYINT NOT NULL,
    Trimestre TINYINT NOT NULL,
    SemanaAno TINYINT NOT NULL,
    DiaSemana TINYINT NOT NULL,
    NomeDia VARCHAR(20) NOT NULL,
    NomeMes VARCHAR(20) NOT NULL,
    IsFimSemana BIT NOT NULL,
    CONSTRAINT UQ_analytics_DimTempo_Data UNIQUE (DataCompleta)
);
GO

CREATE TABLE analytics.DimCurso
(
    CursoKey INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_analytics_DimCurso PRIMARY KEY,
    CursoID INT NOT NULL,
    CodigoCurso VARCHAR(16) NOT NULL,
    NomeCurso NVARCHAR(120) NOT NULL,
    DepartamentoSigla VARCHAR(12) NOT NULL,
    Nivel VARCHAR(20) NOT NULL,
    Modalidade VARCHAR(20) NOT NULL,
    IsCurrent BIT NOT NULL CONSTRAINT DF_analytics_DimCurso_Current DEFAULT (1),
    EffectiveStartDate DATE NOT NULL CONSTRAINT DF_analytics_DimCurso_EffStart DEFAULT CONVERT(DATE, GETDATE()),
    EffectiveEndDate DATE NULL,
    CONSTRAINT UQ_analytics_DimCurso UNIQUE (CursoID, EffectiveStartDate)
);
GO

CREATE TABLE analytics.DimAluno
(
    AlunoKey INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_analytics_DimAluno PRIMARY KEY,
    AlunoID INT NOT NULL,
    RA VARCHAR(20) NOT NULL,
    StatusAcademico VARCHAR(20) NOT NULL,
    Bolsista BIT NOT NULL,
    FaixaBolsa VARCHAR(20) NOT NULL,
    IsCurrent BIT NOT NULL CONSTRAINT DF_analytics_DimAluno_Current DEFAULT (1),
    EffectiveStartDate DATE NOT NULL CONSTRAINT DF_analytics_DimAluno_EffStart DEFAULT CONVERT(DATE, GETDATE()),
    EffectiveEndDate DATE NULL,
    CONSTRAINT UQ_analytics_DimAluno UNIQUE (AlunoID, EffectiveStartDate)
);
GO

CREATE TABLE analytics.DimTurma
(
    TurmaKey INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_analytics_DimTurma PRIMARY KEY,
    TurmaID INT NOT NULL,
    CodigoTurma VARCHAR(24) NOT NULL,
    Ano SMALLINT NOT NULL,
    Periodo TINYINT NOT NULL,
    Turno VARCHAR(10) NOT NULL,
    DisciplinaCodigo VARCHAR(20) NOT NULL,
    DisciplinaNome NVARCHAR(120) NOT NULL,
    CursoID INT NOT NULL,
    ProfessorID INT NOT NULL,
    ProfessorNome NVARCHAR(140) NOT NULL,
    Encerrada BIT NOT NULL,
    CONSTRAINT UQ_analytics_DimTurma UNIQUE (TurmaID)
);
GO

CREATE TABLE analytics.FatoAcademico
(
    FatoAcademicoID BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_analytics_FatoAcademico PRIMARY KEY,
    TempoKey INT NOT NULL,
    AlunoKey INT NOT NULL,
    CursoKey INT NOT NULL,
    TurmaKey INT NOT NULL,
    MatriculaID BIGINT NOT NULL,
    StatusMatricula VARCHAR(20) NOT NULL,
    FrequenciaPercentual DECIMAL(5,2) NOT NULL,
    NotaFinal DECIMAL(5,2) NULL,
    Aprovado BIT NULL,
    CargaHoraria SMALLINT NOT NULL,
    Creditos TINYINT NOT NULL,
    AtualizadoEm DATETIME2(0) NOT NULL CONSTRAINT DF_analytics_FatoAcademico_AtualizadoEm DEFAULT SYSDATETIME(),
    CONSTRAINT UQ_analytics_FatoAcademico_Matricula UNIQUE (MatriculaID),
    CONSTRAINT FK_analytics_FA_Tempo FOREIGN KEY (TempoKey) REFERENCES analytics.DimTempo (TempoKey),
    CONSTRAINT FK_analytics_FA_Aluno FOREIGN KEY (AlunoKey) REFERENCES analytics.DimAluno (AlunoKey),
    CONSTRAINT FK_analytics_FA_Curso FOREIGN KEY (CursoKey) REFERENCES analytics.DimCurso (CursoKey),
    CONSTRAINT FK_analytics_FA_Turma FOREIGN KEY (TurmaKey) REFERENCES analytics.DimTurma (TurmaKey)
);
GO

CREATE TABLE analytics.FatoFinanceiro
(
    FatoFinanceiroID BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_analytics_FatoFinanceiro PRIMARY KEY,
    TempoKey INT NOT NULL,
    AlunoKey INT NOT NULL,
    CursoKey INT NULL,
    ParcelaID BIGINT NOT NULL,
    Competencia CHAR(7) NOT NULL,
    StatusParcela VARCHAR(20) NOT NULL,
    ValorLiquido DECIMAL(12,2) NOT NULL,
    ValorPago DECIMAL(12,2) NOT NULL,
    SaldoAberto DECIMAL(12,2) NOT NULL,
    AtrasoDias INT NOT NULL,
    AtualizadoEm DATETIME2(0) NOT NULL CONSTRAINT DF_analytics_FatoFinanceiro_AtualizadoEm DEFAULT SYSDATETIME(),
    CONSTRAINT UQ_analytics_FatoFinanceiro_Parcela UNIQUE (ParcelaID),
    CONSTRAINT FK_analytics_FF_Tempo FOREIGN KEY (TempoKey) REFERENCES analytics.DimTempo (TempoKey),
    CONSTRAINT FK_analytics_FF_Aluno FOREIGN KEY (AlunoKey) REFERENCES analytics.DimAluno (AlunoKey),
    CONSTRAINT FK_analytics_FF_Curso FOREIGN KEY (CursoKey) REFERENCES analytics.DimCurso (CursoKey)
);
GO

CREATE TABLE analytics.FatoTelemetria
(
    FatoTelemetriaID BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_analytics_FatoTelemetria PRIMARY KEY,
    TempoKey INT NOT NULL,
    EventTypeID INT NOT NULL,
    AlunoKey INT NULL,
    SessionID BIGINT NULL,
    Eventos INT NOT NULL,
    DuracaoMediaMs DECIMAL(18,2) NULL,
    ValorNumericoSoma DECIMAL(18,4) NULL,
    CriadoEm DATETIME2(0) NOT NULL CONSTRAINT DF_analytics_FatoTelemetria_CriadoEm DEFAULT SYSDATETIME(),
    CONSTRAINT FK_analytics_FT_Tempo FOREIGN KEY (TempoKey) REFERENCES analytics.DimTempo (TempoKey),
    CONSTRAINT FK_analytics_FT_EventType FOREIGN KEY (EventTypeID) REFERENCES telemetry.EventType (EventTypeID),
    CONSTRAINT FK_analytics_FT_Aluno FOREIGN KEY (AlunoKey) REFERENCES analytics.DimAluno (AlunoKey),
    CONSTRAINT FK_analytics_FT_Session FOREIGN KEY (SessionID) REFERENCES telemetry.SessionApp (SessionID)
);
GO

CREATE TABLE analytics.KPI_Diario
(
    KPIID BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_analytics_KPI_Diario PRIMARY KEY,
    DataRef DATE NOT NULL,
    KPIName VARCHAR(80) NOT NULL,
    KPIValue DECIMAL(18,4) NOT NULL,
    Unidade VARCHAR(20) NOT NULL,
    MetaValor DECIMAL(18,4) NULL,
    RefreshTime DATETIME2(0) NOT NULL CONSTRAINT DF_analytics_KPI_Diario_Refresh DEFAULT SYSDATETIME(),
    CONSTRAINT UQ_analytics_KPI_Diario UNIQUE (DataRef, KPIName)
);
GO

/* Indices de performance operacional */
CREATE INDEX IX_core_Aluno_Status ON core.Aluno (StatusAcademico, Bolsista);
CREATE INDEX IX_core_Professor_Dep ON core.Professor (DepartamentoID, Ativo);
CREATE INDEX IX_core_Disciplina_Curso ON core.Disciplina (CursoID, SemestreSugerido);
CREATE INDEX IX_core_Turma_Periodo ON core.Turma (Ano, Periodo, Encerrada);
CREATE INDEX IX_core_Matricula_Aluno ON core.Matricula (AlunoID, StatusMatricula) INCLUDE (NotaFinal, FrequenciaPercentual);
CREATE INDEX IX_core_Matricula_Turma ON core.Matricula (TurmaID, StatusMatricula);
CREATE INDEX IX_core_Avaliacao_Turma ON core.Avaliacao (TurmaID, DataAplicacao);
CREATE INDEX IX_core_Nota_Matricula ON core.Nota (MatriculaID) INCLUDE (NotaObtida);
CREATE INDEX IX_core_Parcela_Status ON core.ParcelaFinanceira (StatusParcela, DataVencimento) INCLUDE (ValorLiquido, ValorPago);
CREATE INDEX IX_core_Pagamento_Parcela ON core.Pagamento (ParcelaID, DataPagamento) INCLUDE (ValorPagamento, MeioPagamento);

CREATE INDEX IX_log_ChangeAudit_Time ON log.ChangeAudit (EventTime DESC);
CREATE INDEX IX_log_ProcessRun_Status ON log.ProcessRun (StatusExecucao, IniciadoEm DESC);
CREATE INDEX IX_log_ErrorLog_Time ON log.ErrorLog (CriadoEm DESC);
CREATE INDEX IX_log_AccessLog_Time ON log.AccessLog (CriadoEm DESC, StatusCode);

CREATE INDEX IX_telemetry_EventStream_OccurredAt ON telemetry.EventStream (OccurredAt DESC);
CREATE INDEX IX_telemetry_EventStream_TypeStatus ON telemetry.EventStream (EventTypeID, ProcessingStatus, OccurredAt DESC);
CREATE INDEX IX_telemetry_EventStream_Aluno ON telemetry.EventStream (AlunoID, OccurredAt DESC);
CREATE INDEX IX_telemetry_SessionApp_Aluno ON telemetry.SessionApp (AlunoID, StartedAt DESC);

CREATE INDEX IX_analytics_DimAluno_Current ON analytics.DimAluno (AlunoID, IsCurrent);
CREATE INDEX IX_analytics_DimCurso_Current ON analytics.DimCurso (CursoID, IsCurrent);
CREATE INDEX IX_analytics_FA_Tempo ON analytics.FatoAcademico (TempoKey);
CREATE INDEX IX_analytics_FF_Tempo ON analytics.FatoFinanceiro (TempoKey);
CREATE INDEX IX_analytics_FT_Tempo ON analytics.FatoTelemetria (TempoKey);
CREATE INDEX IX_analytics_KPI_Data ON analytics.KPI_Diario (DataRef DESC);
GO

/* Columnstore para analise em larga escala */
CREATE NONCLUSTERED COLUMNSTORE INDEX NCCI_analytics_FatoAcademico
ON analytics.FatoAcademico
(
    TempoKey, AlunoKey, CursoKey, TurmaKey, StatusMatricula, FrequenciaPercentual, NotaFinal, Aprovado, CargaHoraria, Creditos
);
GO

CREATE NONCLUSTERED COLUMNSTORE INDEX NCCI_analytics_FatoFinanceiro
ON analytics.FatoFinanceiro
(
    TempoKey, AlunoKey, CursoKey, Competencia, StatusParcela, ValorLiquido, ValorPago, SaldoAberto, AtrasoDias
);
GO

CREATE NONCLUSTERED COLUMNSTORE INDEX NCCI_analytics_FatoTelemetria
ON analytics.FatoTelemetria
(
    TempoKey, EventTypeID, AlunoKey, SessionID, Eventos, DuracaoMediaMs, ValorNumericoSoma
);
GO

/* =============== SEED CORE =============== */
INSERT INTO core.Departamento (Nome, Sigla, CentroCusto, EmailContato)
VALUES
    (N'Departamento de Computacao Aplicada', 'COMP', 'CC100', 'comp@faculda.edu'),
    (N'Departamento de Gestao e Negocios', 'GEST', 'CC200', 'gestao@faculda.edu'),
    (N'Departamento de Produto e Design', 'DSGN', 'CC300', 'design@faculda.edu');
GO

INSERT INTO core.Pessoa (NomeExibicao, DocumentoHash, Email, Telefone, DataNascimento)
VALUES
    (N'Docente Seed COMP 01', HASHBYTES('SHA2_256', 'DOC-COMP-01'), 'prof.comp1@faculda.edu', '11995000111', '1985-03-19'),
    (N'Docente Seed GEST 01', HASHBYTES('SHA2_256', 'DOC-GEST-01'), 'prof.gest1@faculda.edu', '11995000112', '1980-09-30'),
    (N'Docente Seed DSGN 01', HASHBYTES('SHA2_256', 'DOC-DSGN-01'), 'prof.dsgn1@faculda.edu', '11995000113', '1977-12-14'),
    (N'Aluno Seed 20260001', HASHBYTES('SHA2_256', 'ALUNO-20260001'), 'aluno.20260001@faculda.edu', '11994000101', '2004-05-10'),
    (N'Aluno Seed 20260002', HASHBYTES('SHA2_256', 'ALUNO-20260002'), 'aluno.20260002@faculda.edu', '11994000102', '2003-11-20'),
    (N'Aluno Seed 20260003', HASHBYTES('SHA2_256', 'ALUNO-20260003'), 'aluno.20260003@faculda.edu', '11994000103', '2005-07-02'),
    (N'Aluno Seed 20260004', HASHBYTES('SHA2_256', 'ALUNO-20260004'), 'aluno.20260004@faculda.edu', '11994000104', '2004-01-28'),
    (N'Aluno Seed 20260005', HASHBYTES('SHA2_256', 'ALUNO-20260005'), 'aluno.20260005@faculda.edu', '11994000105', '2005-02-15');
GO

INSERT INTO core.Professor (PessoaID, DepartamentoID, Titulacao, RegimeTrabalho, DataAdmissao, Ativo)
SELECT p.PessoaID, d.DepartamentoID, 'DOUTORADO', 'INTEGRAL', '2018-02-01', 1
FROM core.Pessoa p JOIN core.Departamento d ON d.Sigla = 'COMP'
WHERE p.Email = 'prof.comp1@faculda.edu'
UNION ALL
SELECT p.PessoaID, d.DepartamentoID, 'MESTRADO', 'PARCIAL', '2020-08-15', 1
FROM core.Pessoa p JOIN core.Departamento d ON d.Sigla = 'GEST'
WHERE p.Email = 'prof.gest1@faculda.edu'
UNION ALL
SELECT p.PessoaID, d.DepartamentoID, 'ESPECIALIZACAO', 'PARCIAL', '2021-01-10', 1
FROM core.Pessoa p JOIN core.Departamento d ON d.Sigla = 'DSGN'
WHERE p.Email = 'prof.dsgn1@faculda.edu';
GO

INSERT INTO core.Curso (DepartamentoID, Codigo, Nome, Nivel, Modalidade, CargaHorariaTotal, SemestresPrevistos, Ativo)
SELECT d.DepartamentoID, 'BSI', N'Sistemas de Informacao', 'GRADUACAO', 'PRESENCIAL', 3200, 8, 1
FROM core.Departamento d WHERE d.Sigla = 'COMP'
UNION ALL
SELECT d.DepartamentoID, 'ADM', N'Administracao Estrategica', 'GRADUACAO', 'PRESENCIAL', 3000, 8, 1
FROM core.Departamento d WHERE d.Sigla = 'GEST'
UNION ALL
SELECT d.DepartamentoID, 'DPD', N'Design de Produto Digital', 'GRADUACAO', 'HIBRIDO', 2800, 8, 1
FROM core.Departamento d WHERE d.Sigla = 'DSGN';
GO

INSERT INTO core.Disciplina (CursoID, Codigo, Nome, CargaHoraria, Creditos, SemestreSugerido, Ementa, Ativa)
SELECT c.CursoID, 'BSI-BD1', N'Banco de Dados I', 80, 4, 3, N'Modelagem relacional, SQL e normalizacao.', 1
FROM core.Curso c WHERE c.Codigo = 'BSI'
UNION ALL
SELECT c.CursoID, 'BSI-WEB', N'Programacao Web', 80, 4, 3, N'Arquitetura web, APIs e observabilidade.', 1
FROM core.Curso c WHERE c.Codigo = 'BSI'
UNION ALL
SELECT c.CursoID, 'ADM-AF', N'Analise Financeira', 60, 3, 2, N'Fluxo de caixa, modelagem de risco e KPI.', 1
FROM core.Curso c WHERE c.Codigo = 'ADM'
UNION ALL
SELECT c.CursoID, 'DPD-UX', N'UX Strategy', 60, 3, 2, N'Pesquisa de usuario, prototipos e metricas.', 1
FROM core.Curso c WHERE c.Codigo = 'DPD';
GO

INSERT INTO core.Turma (DisciplinaID, ProfessorID, CodigoTurma, Ano, Periodo, Turno, Vagas, DataInicio, DataFim, Encerrada)
SELECT d.DisciplinaID, pr.ProfessorID, 'BSI-BD1-2026A', 2026, 1, 'NOITE', 45, '2026-02-10', '2026-06-30', 0
FROM core.Disciplina d
JOIN core.Professor pr ON pr.PessoaID = (SELECT PessoaID FROM core.Pessoa WHERE Email = 'prof.comp1@faculda.edu')
WHERE d.Codigo = 'BSI-BD1'
UNION ALL
SELECT d.DisciplinaID, pr.ProfessorID, 'BSI-WEB-2026A', 2026, 1, 'NOITE', 40, '2026-02-10', '2026-06-30', 0
FROM core.Disciplina d
JOIN core.Professor pr ON pr.PessoaID = (SELECT PessoaID FROM core.Pessoa WHERE Email = 'prof.comp1@faculda.edu')
WHERE d.Codigo = 'BSI-WEB'
UNION ALL
SELECT d.DisciplinaID, pr.ProfessorID, 'ADM-AF-2026A', 2026, 1, 'MANHA', 50, '2026-02-10', '2026-06-30', 0
FROM core.Disciplina d
JOIN core.Professor pr ON pr.PessoaID = (SELECT PessoaID FROM core.Pessoa WHERE Email = 'prof.gest1@faculda.edu')
WHERE d.Codigo = 'ADM-AF'
UNION ALL
SELECT d.DisciplinaID, pr.ProfessorID, 'DPD-UX-2026A', 2026, 1, 'TARDE', 35, '2026-02-10', '2026-06-30', 0
FROM core.Disciplina d
JOIN core.Professor pr ON pr.PessoaID = (SELECT PessoaID FROM core.Pessoa WHERE Email = 'prof.dsgn1@faculda.edu')
WHERE d.Codigo = 'DPD-UX';
GO
INSERT INTO core.Aluno (PessoaID, RA, DataIngresso, StatusAcademico, Bolsista, PercentualBolsa)
SELECT PessoaID, '20260001', '2026-02-01', 'ATIVO', 1, 40 FROM core.Pessoa WHERE Email = 'aluno.20260001@faculda.edu'
UNION ALL
SELECT PessoaID, '20260002', '2026-02-01', 'ATIVO', 0, 0 FROM core.Pessoa WHERE Email = 'aluno.20260002@faculda.edu'
UNION ALL
SELECT PessoaID, '20260003', '2026-02-01', 'ATIVO', 1, 60 FROM core.Pessoa WHERE Email = 'aluno.20260003@faculda.edu'
UNION ALL
SELECT PessoaID, '20260004', '2026-02-01', 'ATIVO', 0, 0 FROM core.Pessoa WHERE Email = 'aluno.20260004@faculda.edu'
UNION ALL
SELECT PessoaID, '20260005', '2026-02-01', 'ATIVO', 0, 0 FROM core.Pessoa WHERE Email = 'aluno.20260005@faculda.edu';
GO

INSERT INTO core.Matricula (AlunoID, TurmaID, OrigemMatricula, StatusMatricula, FrequenciaPercentual, NotaFinal)
SELECT a.AlunoID, t.TurmaID, 'PORTAL', 'APROVADA', 92.50, 8.70
FROM core.Aluno a JOIN core.Turma t ON t.CodigoTurma = 'BSI-BD1-2026A'
WHERE a.RA = '20260001'
UNION ALL
SELECT a.AlunoID, t.TurmaID, 'PORTAL', 'APROVADA', 88.00, 7.90
FROM core.Aluno a JOIN core.Turma t ON t.CodigoTurma = 'BSI-BD1-2026A'
WHERE a.RA = '20260002'
UNION ALL
SELECT a.AlunoID, t.TurmaID, 'SECRETARIA', 'REPROVADA', 70.00, 5.20
FROM core.Aluno a JOIN core.Turma t ON t.CodigoTurma = 'BSI-BD1-2026A'
WHERE a.RA = '20260004'
UNION ALL
SELECT a.AlunoID, t.TurmaID, 'PORTAL', 'ATIVA', 80.00, NULL
FROM core.Aluno a JOIN core.Turma t ON t.CodigoTurma = 'ADM-AF-2026A'
WHERE a.RA = '20260003'
UNION ALL
SELECT a.AlunoID, t.TurmaID, 'API', 'ATIVA', 85.00, NULL
FROM core.Aluno a JOIN core.Turma t ON t.CodigoTurma = 'DPD-UX-2026A'
WHERE a.RA = '20260005'
UNION ALL
SELECT a.AlunoID, t.TurmaID, 'PORTAL', 'ATIVA', 90.00, NULL
FROM core.Aluno a JOIN core.Turma t ON t.CodigoTurma = 'BSI-WEB-2026A'
WHERE a.RA = '20260001';
GO

INSERT INTO core.Avaliacao (TurmaID, NomeAvaliacao, TipoAvaliacao, Peso, NotaMaxima, DataAplicacao)
SELECT t.TurmaID, N'Prova 1', 'PROVA', 40, 10, '2026-04-10'
FROM core.Turma t WHERE t.CodigoTurma = 'BSI-BD1-2026A'
UNION ALL
SELECT t.TurmaID, N'Projeto Final', 'PROJETO', 60, 10, '2026-06-20'
FROM core.Turma t WHERE t.CodigoTurma = 'BSI-BD1-2026A'
UNION ALL
SELECT t.TurmaID, N'Case Empresarial', 'TRABALHO', 100, 10, '2026-06-15'
FROM core.Turma t WHERE t.CodigoTurma = 'ADM-AF-2026A'
UNION ALL
SELECT t.TurmaID, N'Portfolio UX', 'PROJETO', 100, 10, '2026-06-18'
FROM core.Turma t WHERE t.CodigoTurma = 'DPD-UX-2026A';
GO

INSERT INTO core.Nota (AvaliacaoID, MatriculaID, NotaObtida, Comentario)
SELECT av.AvaliacaoID, m.MatriculaID, 8.50, N'Bom dominio da modelagem.'
FROM core.Avaliacao av
JOIN core.Turma t ON t.TurmaID = av.TurmaID
JOIN core.Matricula m ON m.TurmaID = t.TurmaID
JOIN core.Aluno a ON a.AlunoID = m.AlunoID
WHERE av.NomeAvaliacao = N'Prova 1' AND t.CodigoTurma = 'BSI-BD1-2026A' AND a.RA = '20260001'
UNION ALL
SELECT av.AvaliacaoID, m.MatriculaID, 8.90, N'Projeto robusto.'
FROM core.Avaliacao av
JOIN core.Turma t ON t.TurmaID = av.TurmaID
JOIN core.Matricula m ON m.TurmaID = t.TurmaID
JOIN core.Aluno a ON a.AlunoID = m.AlunoID
WHERE av.NomeAvaliacao = N'Projeto Final' AND t.CodigoTurma = 'BSI-BD1-2026A' AND a.RA = '20260001'
UNION ALL
SELECT av.AvaliacaoID, m.MatriculaID, 7.30, N'Boa evolucao tecnica.'
FROM core.Avaliacao av
JOIN core.Turma t ON t.TurmaID = av.TurmaID
JOIN core.Matricula m ON m.TurmaID = t.TurmaID
JOIN core.Aluno a ON a.AlunoID = m.AlunoID
WHERE av.NomeAvaliacao = N'Prova 1' AND t.CodigoTurma = 'BSI-BD1-2026A' AND a.RA = '20260002'
UNION ALL
SELECT av.AvaliacaoID, m.MatriculaID, 8.30, N'Entrega consistente.'
FROM core.Avaliacao av
JOIN core.Turma t ON t.TurmaID = av.TurmaID
JOIN core.Matricula m ON m.TurmaID = t.TurmaID
JOIN core.Aluno a ON a.AlunoID = m.AlunoID
WHERE av.NomeAvaliacao = N'Projeto Final' AND t.CodigoTurma = 'BSI-BD1-2026A' AND a.RA = '20260002'
UNION ALL
SELECT av.AvaliacaoID, m.MatriculaID, 5.00, N'Dificuldade com consultas analiticas.'
FROM core.Avaliacao av
JOIN core.Turma t ON t.TurmaID = av.TurmaID
JOIN core.Matricula m ON m.TurmaID = t.TurmaID
JOIN core.Aluno a ON a.AlunoID = m.AlunoID
WHERE av.NomeAvaliacao = N'Prova 1' AND t.CodigoTurma = 'BSI-BD1-2026A' AND a.RA = '20260004'
UNION ALL
SELECT av.AvaliacaoID, m.MatriculaID, 5.30, N'Projeto incompleto.'
FROM core.Avaliacao av
JOIN core.Turma t ON t.TurmaID = av.TurmaID
JOIN core.Matricula m ON m.TurmaID = t.TurmaID
JOIN core.Aluno a ON a.AlunoID = m.AlunoID
WHERE av.NomeAvaliacao = N'Projeto Final' AND t.CodigoTurma = 'BSI-BD1-2026A' AND a.RA = '20260004';
GO

INSERT INTO core.PlanoFinanceiro (CursoID, AnoReferencia, ValorMensalBase, DiaVencimento, PercentualMulta, PercentualJurosMes, Ativo)
SELECT CursoID, 2026, 1350.00, 10, 2.00, 1.00, 1 FROM core.Curso WHERE Codigo = 'BSI'
UNION ALL
SELECT CursoID, 2026, 1200.00, 10, 2.00, 1.00, 1 FROM core.Curso WHERE Codigo = 'ADM'
UNION ALL
SELECT CursoID, 2026, 1280.00, 10, 2.00, 1.00, 1 FROM core.Curso WHERE Codigo = 'DPD';
GO

INSERT INTO core.ParcelaFinanceira
(
    AlunoID, PlanoFinanceiroID, Competencia, DataVencimento,
    ValorOriginal, ValorDesconto, ValorMulta, ValorPago, DataPagamento, StatusParcela, CanalGeracao, Observacao
)
SELECT a.AlunoID, pf.PlanoFinanceiroID, '2026-03', '2026-03-10', 1350.00, 540.00, 0, 810.00, '2026-03-09', 'PAGA', 'MENSALIDADE', N'Bolsa 40%.'
FROM core.Aluno a
JOIN core.PlanoFinanceiro pf ON pf.CursoID = (SELECT CursoID FROM core.Curso WHERE Codigo = 'BSI')
WHERE a.RA = '20260001'
UNION ALL
SELECT a.AlunoID, pf.PlanoFinanceiroID, '2026-04', '2026-04-10', 1350.00, 540.00, 0, NULL, NULL, 'ABERTA', 'MENSALIDADE', N'Em aberto.'
FROM core.Aluno a
JOIN core.PlanoFinanceiro pf ON pf.CursoID = (SELECT CursoID FROM core.Curso WHERE Codigo = 'BSI')
WHERE a.RA = '20260001'
UNION ALL
SELECT a.AlunoID, pf.PlanoFinanceiroID, '2026-03', '2026-03-10', 1350.00, 0, 20.00, NULL, NULL, 'ATRASADA', 'MENSALIDADE', N'Parcela vencida.'
FROM core.Aluno a
JOIN core.PlanoFinanceiro pf ON pf.CursoID = (SELECT CursoID FROM core.Curso WHERE Codigo = 'BSI')
WHERE a.RA = '20260002'
UNION ALL
SELECT a.AlunoID, pf.PlanoFinanceiroID, '2026-03', '2026-03-10', 1200.00, 720.00, 0, 480.00, '2026-03-10', 'PAGA', 'MENSALIDADE', N'Bolsa 60%.'
FROM core.Aluno a
JOIN core.PlanoFinanceiro pf ON pf.CursoID = (SELECT CursoID FROM core.Curso WHERE Codigo = 'ADM')
WHERE a.RA = '20260003'
UNION ALL
SELECT a.AlunoID, pf.PlanoFinanceiroID, '2026-03', '2026-03-10', 1350.00, 0, 0, 1350.00, '2026-03-08', 'PAGA', 'MENSALIDADE', N'Pagamento integral.'
FROM core.Aluno a
JOIN core.PlanoFinanceiro pf ON pf.CursoID = (SELECT CursoID FROM core.Curso WHERE Codigo = 'BSI')
WHERE a.RA = '20260004'
UNION ALL
SELECT a.AlunoID, pf.PlanoFinanceiroID, '2026-03', '2026-03-10', 1280.00, 0, 0, NULL, NULL, 'ABERTA', 'MENSALIDADE', N'Primeira mensalidade.'
FROM core.Aluno a
JOIN core.PlanoFinanceiro pf ON pf.CursoID = (SELECT CursoID FROM core.Curso WHERE Codigo = 'DPD')
WHERE a.RA = '20260005';
GO

INSERT INTO core.Pagamento (ParcelaID, DataPagamento, ValorPagamento, MeioPagamento, GatewayStatus, ReferenciaExterna, UsuarioLancamento, Observacao)
SELECT p.ParcelaID, '2026-03-09T10:40:00', 810.00, 'PIX', 'CONFIRMED', 'TXN-20260309-0001', N'finance.ops', N'Pagamento instantaneo.'
FROM core.ParcelaFinanceira p JOIN core.Aluno a ON a.AlunoID = p.AlunoID
WHERE a.RA = '20260001' AND p.Competencia = '2026-03'
UNION ALL
SELECT p.ParcelaID, '2026-03-10T09:00:00', 480.00, 'BOLETO', 'CONFIRMED', 'BLT-20260310-0098', N'finance.ops', N'Compensacao D+0.'
FROM core.ParcelaFinanceira p JOIN core.Aluno a ON a.AlunoID = p.AlunoID
WHERE a.RA = '20260003' AND p.Competencia = '2026-03'
UNION ALL
SELECT p.ParcelaID, '2026-03-08T16:25:00', 1350.00, 'CARTAO', 'CONFIRMED', 'CRD-20260308-1230', N'finance.ops', N'Autorizacao full.'
FROM core.ParcelaFinanceira p JOIN core.Aluno a ON a.AlunoID = p.AlunoID
WHERE a.RA = '20260004' AND p.Competencia = '2026-03';
GO

/* =============== SEED SECURITY/LOG/TELEMETRY =============== */
INSERT INTO security.UsuarioSistema (Login, NomeExibicao, Email, Perfil, Ativo)
VALUES
    ('admin.master', N'Operador Admin', 'admin@faculda.edu', 'ADMIN', 1),
    ('core.ops', N'Operador Core', 'core.ops@faculda.edu', 'CORE_OPS', 1),
    ('finance.ops', N'Operador Financeiro', 'finance.ops@faculda.edu', 'FINANCE_OPS', 1),
    ('analytics.ops', N'Operador Analytics', 'analytics.ops@faculda.edu', 'ANALYTICS', 1),
    ('observer.ops', N'Observador', 'observer@faculda.edu', 'OBSERVER', 1);
GO

INSERT INTO telemetry.EventType (EventCode, Categoria, Severidade, Ativo)
VALUES
    ('SESSION_START', 'AUTH', 1, 1),
    ('LOGIN_SUCCESS', 'AUTH', 1, 1),
    ('LOGIN_FAILED', 'AUTH', 3, 1),
    ('PAGE_VIEW', 'USAGE', 0, 1),
    ('API_CALL', 'USAGE', 1, 1),
    ('MATRICULA_CREATED', 'ACADEMIC', 1, 1),
    ('NOTE_UPDATED', 'ACADEMIC', 1, 1),
    ('PAYMENT_ATTEMPT', 'FINANCE', 2, 1),
    ('PAYMENT_SUCCESS', 'FINANCE', 1, 1),
    ('PAYMENT_FAILED', 'FINANCE', 4, 1);
GO

INSERT INTO telemetry.SessionApp (AlunoID, UsuarioID, ClientID, Platform, AppVersion, StartedAt, EndedAt, IsAuthenticated, IpAddress, CountryCode)
SELECT a.AlunoID, NULL, 'web-client-001', 'WEB', '2.3.1', '2026-03-09T10:00:00', '2026-03-09T11:20:00', 1, '200.155.10.1', 'BR'
FROM core.Aluno a WHERE a.RA = '20260001'
UNION ALL
SELECT a.AlunoID, NULL, 'mobile-client-002', 'MOBILE', '5.1.0', '2026-03-09T10:05:00', '2026-03-09T10:40:00', 1, '200.155.10.2', 'BR'
FROM core.Aluno a WHERE a.RA = '20260002'
UNION ALL
SELECT NULL, u.UsuarioID, 'batch-analytics-001', 'BATCH', '1.0.0', '2026-03-09T00:00:00', '2026-03-09T00:05:00', 1, '10.0.0.15', 'BR'
FROM security.UsuarioSistema u WHERE u.Login = 'analytics.ops';
GO

INSERT INTO telemetry.EventStream
(
    EventTypeID, SessionID, AlunoID, OccurredAt, SourceSystem, EventKey, DurationMs, NumericValue, BooleanValue, PayloadJson, ProcessingStatus
)
SELECT et.EventTypeID, s.SessionID, s.AlunoID, '2026-03-09T10:00:01', 'PORTAL', 'sess-open', 50, NULL, 1, N'{"route":"login","result":"ok"}', 'N'
FROM telemetry.EventType et
JOIN telemetry.SessionApp s ON s.ClientID = 'web-client-001'
WHERE et.EventCode = 'SESSION_START'
UNION ALL
SELECT et.EventTypeID, s.SessionID, s.AlunoID, '2026-03-09T10:00:05', 'PORTAL', 'auth-ok', 120, NULL, 1, N'{"method":"password","result":"ok"}', 'N'
FROM telemetry.EventType et
JOIN telemetry.SessionApp s ON s.ClientID = 'web-client-001'
WHERE et.EventCode = 'LOGIN_SUCCESS'
UNION ALL
SELECT et.EventTypeID, s.SessionID, s.AlunoID, '2026-03-09T10:10:00', 'PORTAL', 'view-dashboard', 35, NULL, NULL, N'{"route":"/dashboard"}', 'N'
FROM telemetry.EventType et
JOIN telemetry.SessionApp s ON s.ClientID = 'web-client-001'
WHERE et.EventCode = 'PAGE_VIEW'
UNION ALL
SELECT et.EventTypeID, s.SessionID, s.AlunoID, '2026-03-09T10:12:00', 'PORTAL', 'matricula-create', 180, NULL, 1, N'{"turma":"BSI-WEB-2026A"}', 'N'
FROM telemetry.EventType et
JOIN telemetry.SessionApp s ON s.ClientID = 'web-client-001'
WHERE et.EventCode = 'MATRICULA_CREATED'
UNION ALL
SELECT et.EventTypeID, s.SessionID, s.AlunoID, '2026-03-09T10:15:00', 'PORTAL', 'pay-attempt', 220, 810.00, 1, N'{"gateway":"pix","status":"confirmed"}', 'N'
FROM telemetry.EventType et
JOIN telemetry.SessionApp s ON s.ClientID = 'web-client-001'
WHERE et.EventCode = 'PAYMENT_SUCCESS'
UNION ALL
SELECT et.EventTypeID, s.SessionID, s.AlunoID, '2026-03-09T10:16:00', 'PORTAL', 'api-get-turmas', 90, NULL, 1, N'{"resource":"/api/turmas","httpStatus":200}', 'N'
FROM telemetry.EventType et
JOIN telemetry.SessionApp s ON s.ClientID = 'mobile-client-002'
WHERE et.EventCode = 'API_CALL'
UNION ALL
SELECT et.EventTypeID, s.SessionID, s.AlunoID, '2026-03-09T10:16:20', 'PORTAL', 'auth-fail', 110, NULL, 0, N'{"result":"invalid_password"}', 'N'
FROM telemetry.EventType et
JOIN telemetry.SessionApp s ON s.ClientID = 'mobile-client-002'
WHERE et.EventCode = 'LOGIN_FAILED';
GO

INSERT INTO log.AccessLog (UsuarioID, LoginEfetivo, Acao, Recurso, IpOrigem, UserAgent, StatusCode, DuracaoMs)
SELECT u.UsuarioID, 'analytics.ops', 'EXECUTE', '/analytics/sp_RebuildWarehouse', '10.0.0.15', 'sqlcmd', 200, 485
FROM security.UsuarioSistema u WHERE u.Login = 'analytics.ops'
UNION ALL
SELECT u.UsuarioID, 'finance.ops', 'INSERT', '/core/pagamento', '10.0.0.20', 'ssms', 201, 122
FROM security.UsuarioSistema u WHERE u.Login = 'finance.ops';
GO

/* Seed de dimensao de tempo (3 anos) */
;WITH Datas AS
(
    SELECT CONVERT(DATE, '2025-01-01') AS DataRef
    UNION ALL
    SELECT DATEADD(DAY, 1, DataRef)
    FROM Datas
    WHERE DataRef < '2027-12-31'
)
INSERT INTO analytics.DimTempo
(
    TempoKey, DataCompleta, Ano, Mes, Dia, Trimestre, SemanaAno, DiaSemana, NomeDia, NomeMes, IsFimSemana
)
SELECT
    CONVERT(INT, FORMAT(DataRef, 'yyyyMMdd')) AS TempoKey,
    DataRef,
    DATEPART(YEAR, DataRef) AS Ano,
    DATEPART(MONTH, DataRef) AS Mes,
    DATEPART(DAY, DataRef) AS Dia,
    DATEPART(QUARTER, DataRef) AS Trimestre,
    DATEPART(WEEK, DataRef) AS SemanaAno,
    DATEPART(WEEKDAY, DataRef) AS DiaSemana,
    DATENAME(WEEKDAY, DataRef) AS NomeDia,
    DATENAME(MONTH, DataRef) AS NomeMes,
    CASE WHEN DATEPART(WEEKDAY, DataRef) IN (1,7) THEN 1 ELSE 0 END AS IsFimSemana
FROM Datas
OPTION (MAXRECURSION 0);
GO

PRINT N'Base [Faculda] criada em camadas: CORE + LOG + TELEMETRY + ANALYTICS.';
GO
