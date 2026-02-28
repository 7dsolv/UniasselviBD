/*
Projeto: Faculda - Mapa do Tesouro de Dados
Uniasselvi: Adilson Oliveira
Descricao: Complementacao enterprise (programmability + observability + analytics)
Autor: Adilson Oliveira
Data: 2026-02-28
*/

SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

IF DB_ID(N'Faculda') IS NULL
BEGIN
    THROW 50000, N'Banco [Faculda] nao encontrado. Execute primeiro faculda.sql.', 1;
END;
GO

USE [Faculda];
GO

IF OBJECT_ID(N'core.Aluno', N'U') IS NULL
BEGIN
    THROW 50001, N'Camada CORE nao encontrada. Execute primeiro faculda.sql.', 1;
END;
GO

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

/* =============== FUNCOES =============== */
CREATE OR ALTER FUNCTION core.fn_CRA_Aluno
(
    @AlunoID INT
)
RETURNS DECIMAL(5,2)
AS
BEGIN
    DECLARE @CRA DECIMAL(5,2);

    SELECT
        @CRA = CAST(
            ROUND(
                SUM(ISNULL(m.NotaFinal, 0) * d.Creditos) / NULLIF(SUM(d.Creditos), 0),
                2
            ) AS DECIMAL(5,2)
        )
    FROM core.Matricula m
    JOIN core.Turma t ON t.TurmaID = m.TurmaID
    JOIN core.Disciplina d ON d.DisciplinaID = t.DisciplinaID
    WHERE
        m.AlunoID = @AlunoID
        AND m.StatusMatricula IN ('APROVADA', 'REPROVADA');

    RETURN ISNULL(@CRA, 0);
END;
GO

CREATE OR ALTER FUNCTION core.fn_SaldoAluno
(
    @AlunoID INT
)
RETURNS TABLE
AS
RETURN
(
    SELECT
        @AlunoID AS AlunoID,
        COUNT_BIG(*) AS ParcelasTotal,
        SUM(CASE WHEN pf.StatusParcela IN ('ABERTA', 'ATRASADA', 'NEGOCIADA') THEN 1 ELSE 0 END) AS ParcelasPendentes,
        CAST(SUM(CASE WHEN pf.StatusParcela IN ('ABERTA', 'ATRASADA', 'NEGOCIADA') THEN pf.ValorLiquido - ISNULL(pf.ValorPago, 0) ELSE 0 END) AS DECIMAL(12,2)) AS ValorEmAberto,
        CAST(SUM(ISNULL(pf.ValorPago, 0)) AS DECIMAL(12,2)) AS ValorPago
    FROM core.ParcelaFinanceira pf
    WHERE pf.AlunoID = @AlunoID
);
GO

CREATE OR ALTER FUNCTION analytics.fn_TempoKey
(
    @DataRef DATE
)
RETURNS INT
AS
BEGIN
    RETURN CONVERT(INT, FORMAT(@DataRef, 'yyyyMMdd'));
END;
GO

/* =============== PROCEDURES DE SUPORTE =============== */
CREATE OR ALTER PROCEDURE log.sp_RegisterError
    @ProcessoNome VARCHAR(120),
    @Etapa VARCHAR(120) = NULL,
    @NumeroErro INT = NULL,
    @Severidade SMALLINT = NULL,
    @Estado SMALLINT = NULL,
    @ProcedureName SYSNAME = NULL,
    @Linha INT = NULL,
    @Mensagem NVARCHAR(MAX),
    @Payload NVARCHAR(MAX) = NULL,
    @CorrelationID UNIQUEIDENTIFIER = NULL
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO log.ErrorLog
    (
        ProcessoNome,
        Etapa,
        NumeroErro,
        Severidade,
        Estado,
        ProcedureName,
        Linha,
        Mensagem,
        Payload,
        CorrelationID
    )
    VALUES
    (
        @ProcessoNome,
        @Etapa,
        @NumeroErro,
        @Severidade,
        @Estado,
        @ProcedureName,
        @Linha,
        @Mensagem,
        @Payload,
        @CorrelationID
    );
END;
GO

CREATE OR ALTER PROCEDURE telemetry.sp_StartSession
    @ClientID VARCHAR(80),
    @Platform VARCHAR(20),
    @AppVersion VARCHAR(30) = NULL,
    @AlunoID INT = NULL,
    @UsuarioID INT = NULL,
    @IpAddress VARCHAR(45) = NULL,
    @CountryCode CHAR(2) = NULL,
    @IsAuthenticated BIT = 0,
    @SessionID BIGINT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF @Platform NOT IN ('WEB', 'MOBILE', 'DESKTOP', 'API', 'BATCH')
    BEGIN
        THROW 51040, N'Platform invalida.', 1;
    END;

    INSERT INTO telemetry.SessionApp
    (
        AlunoID,
        UsuarioID,
        ClientID,
        Platform,
        AppVersion,
        IsAuthenticated,
        IpAddress,
        CountryCode
    )
    VALUES
    (
        @AlunoID,
        @UsuarioID,
        @ClientID,
        @Platform,
        @AppVersion,
        @IsAuthenticated,
        @IpAddress,
        @CountryCode
    );

    SET @SessionID = CAST(SCOPE_IDENTITY() AS BIGINT);

    DECLARE @EventTypeID INT;
    SELECT @EventTypeID = EventTypeID
    FROM telemetry.EventType
    WHERE EventCode = 'SESSION_START';

    IF @EventTypeID IS NOT NULL
    BEGIN
        INSERT INTO telemetry.EventStream
        (
            EventTypeID,
            SessionID,
            AlunoID,
            SourceSystem,
            EventKey,
            PayloadJson
        )
        VALUES
        (
            @EventTypeID,
            @SessionID,
            @AlunoID,
            'SESSION_SERVICE',
            CONCAT('start-', @SessionID),
            CONCAT(N'{"clientId":"', @ClientID, N'","platform":"', @Platform, N'"}')
        );
    END;

    SELECT @SessionID AS SessionID;
END;
GO

CREATE OR ALTER PROCEDURE telemetry.sp_IngerirEvento
    @EventCode VARCHAR(80),
    @SourceSystem VARCHAR(40),
    @SessionID BIGINT = NULL,
    @AlunoID INT = NULL,
    @OccurredAt DATETIME2(0) = NULL,
    @EventKey VARCHAR(100) = NULL,
    @DurationMs INT = NULL,
    @NumericValue DECIMAL(18,4) = NULL,
    @BooleanValue BIT = NULL,
    @PayloadJson NVARCHAR(MAX) = NULL,
    @CorrelationID UNIQUEIDENTIFIER = NULL,
    @EventID BIGINT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @EventTypeID INT;

    SELECT @EventTypeID = EventTypeID
    FROM telemetry.EventType
    WHERE EventCode = @EventCode AND Ativo = 1;

    IF @EventTypeID IS NULL
    BEGIN
        THROW 51041, N'EventCode nao cadastrado ou inativo.', 1;
    END;

    IF @PayloadJson IS NOT NULL AND ISJSON(@PayloadJson) <> 1
    BEGIN
        THROW 51042, N'PayloadJson invalido.', 1;
    END;

    IF @OccurredAt IS NULL
    BEGIN
        SET @OccurredAt = SYSDATETIME();
    END;

    IF @CorrelationID IS NULL
    BEGIN
        SET @CorrelationID = NEWID();
    END;

    INSERT INTO telemetry.EventStream
    (
        EventTypeID,
        SessionID,
        AlunoID,
        OccurredAt,
        SourceSystem,
        CorrelationID,
        EventKey,
        DurationMs,
        NumericValue,
        BooleanValue,
        PayloadJson
    )
    VALUES
    (
        @EventTypeID,
        @SessionID,
        @AlunoID,
        @OccurredAt,
        @SourceSystem,
        @CorrelationID,
        @EventKey,
        @DurationMs,
        @NumericValue,
        @BooleanValue,
        @PayloadJson
    );

    SET @EventID = CAST(SCOPE_IDENTITY() AS BIGINT);

    SELECT @EventID AS EventID, @CorrelationID AS CorrelationID;
END;
GO
/* =============== PROCEDURES CORE =============== */
CREATE OR ALTER PROCEDURE core.sp_MatricularAluno
    @AlunoID INT,
    @TurmaID INT,
    @OrigemMatricula VARCHAR(20) = 'PORTAL',
    @MatriculaID BIGINT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF @OrigemMatricula NOT IN ('PORTAL', 'SECRETARIA', 'API', 'BATCH')
    BEGIN
        THROW 51000, N'Origem de matricula invalida.', 1;
    END;

    IF NOT EXISTS
    (
        SELECT 1
        FROM core.Aluno
        WHERE AlunoID = @AlunoID AND StatusAcademico = 'ATIVO'
    )
    BEGIN
        THROW 51001, N'Aluno invalido ou inativo.', 1;
    END;

    IF NOT EXISTS
    (
        SELECT 1
        FROM core.Turma
        WHERE TurmaID = @TurmaID AND Encerrada = 0
    )
    BEGIN
        THROW 51002, N'Turma invalida ou encerrada.', 1;
    END;

    IF EXISTS
    (
        SELECT 1
        FROM core.Matricula
        WHERE AlunoID = @AlunoID AND TurmaID = @TurmaID
    )
    BEGIN
        THROW 51003, N'O aluno ja possui matricula nesta turma.', 1;
    END;

    DECLARE @Vagas SMALLINT;
    DECLARE @Ocupadas INT;

    SELECT @Vagas = Vagas
    FROM core.Turma
    WHERE TurmaID = @TurmaID;

    SELECT
        @Ocupadas = COUNT(*)
    FROM core.Matricula
    WHERE
        TurmaID = @TurmaID
        AND StatusMatricula IN ('ATIVA', 'APROVADA', 'REPROVADA');

    IF @Ocupadas >= @Vagas
    BEGIN
        THROW 51004, N'Turma sem vagas disponiveis.', 1;
    END;

    INSERT INTO core.Matricula (AlunoID, TurmaID, OrigemMatricula)
    VALUES (@AlunoID, @TurmaID, @OrigemMatricula);

    SET @MatriculaID = CAST(SCOPE_IDENTITY() AS BIGINT);

    DECLARE @EventTypeID INT;
    SELECT @EventTypeID = EventTypeID FROM telemetry.EventType WHERE EventCode = 'MATRICULA_CREATED';

    IF @EventTypeID IS NOT NULL
    BEGIN
        INSERT INTO telemetry.EventStream
        (
            EventTypeID,
            AlunoID,
            SourceSystem,
            EventKey,
            PayloadJson
        )
        VALUES
        (
            @EventTypeID,
            @AlunoID,
            'CORE.SP_MATRICULARALUNO',
            CONCAT('matricula-', @MatriculaID),
            CONCAT(N'{"matriculaId":', @MatriculaID, N',"turmaId":', @TurmaID, N'}')
        );
    END;

    SELECT *
    FROM core.Matricula
    WHERE MatriculaID = @MatriculaID;
END;
GO

CREATE OR ALTER PROCEDURE core.sp_LancarNotaFinal
    @MatriculaID BIGINT,
    @NotaFinal DECIMAL(5,2),
    @FrequenciaPercentual DECIMAL(5,2)
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF @NotaFinal < 0 OR @NotaFinal > 10
    BEGIN
        THROW 51010, N'Nota final deve estar entre 0 e 10.', 1;
    END;

    IF @FrequenciaPercentual < 0 OR @FrequenciaPercentual > 100
    BEGIN
        THROW 51011, N'Frequencia deve estar entre 0 e 100.', 1;
    END;

    IF NOT EXISTS (SELECT 1 FROM core.Matricula WHERE MatriculaID = @MatriculaID)
    BEGIN
        THROW 51012, N'Matricula nao encontrada.', 1;
    END;

    UPDATE core.Matricula
    SET
        NotaFinal = @NotaFinal,
        FrequenciaPercentual = @FrequenciaPercentual,
        StatusMatricula = CASE
            WHEN @FrequenciaPercentual >= 75 AND @NotaFinal >= 6 THEN 'APROVADA'
            ELSE 'REPROVADA'
        END,
        UltimaAtualizacao = SYSDATETIME()
    WHERE
        MatriculaID = @MatriculaID
        AND StatusMatricula NOT IN ('CANCELADA', 'TRANCADA');

    IF @@ROWCOUNT = 0
    BEGIN
        THROW 51013, N'Matricula nao pode ser atualizada (status bloqueado).', 1;
    END;

    DECLARE @AlunoID INT;
    SELECT @AlunoID = AlunoID FROM core.Matricula WHERE MatriculaID = @MatriculaID;

    DECLARE @EventTypeID INT;
    SELECT @EventTypeID = EventTypeID FROM telemetry.EventType WHERE EventCode = 'NOTE_UPDATED';

    IF @EventTypeID IS NOT NULL
    BEGIN
        INSERT INTO telemetry.EventStream
        (
            EventTypeID,
            AlunoID,
            SourceSystem,
            EventKey,
            NumericValue,
            PayloadJson
        )
        VALUES
        (
            @EventTypeID,
            @AlunoID,
            'CORE.SP_LANCARNOTAFINAL',
            CONCAT('matricula-', @MatriculaID),
            @NotaFinal,
            CONCAT(N'{"matriculaId":', @MatriculaID, N',"frequencia":', CONVERT(VARCHAR(10), @FrequenciaPercentual), N'}')
        );
    END;

    SELECT *
    FROM core.Matricula
    WHERE MatriculaID = @MatriculaID;
END;
GO

CREATE OR ALTER PROCEDURE core.sp_RegistrarPagamento
    @ParcelaID BIGINT,
    @ValorPagamento DECIMAL(10,2),
    @MeioPagamento VARCHAR(20),
    @ReferenciaExterna VARCHAR(80) = NULL,
    @UsuarioLancamento NVARCHAR(80) = NULL,
    @Observacao NVARCHAR(250) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF @ValorPagamento <= 0
    BEGIN
        THROW 51020, N'Valor de pagamento invalido.', 1;
    END;

    IF @MeioPagamento NOT IN ('PIX', 'CARTAO', 'BOLETO', 'TRANSFERENCIA', 'DINHEIRO')
    BEGIN
        THROW 51021, N'Meio de pagamento invalido.', 1;
    END;

    IF NOT EXISTS (SELECT 1 FROM core.ParcelaFinanceira WHERE ParcelaID = @ParcelaID)
    BEGIN
        THROW 51022, N'Parcela nao encontrada.', 1;
    END;

    IF EXISTS (SELECT 1 FROM core.ParcelaFinanceira WHERE ParcelaID = @ParcelaID AND StatusParcela = 'CANCELADA')
    BEGIN
        THROW 51023, N'Parcela cancelada nao pode receber pagamento.', 1;
    END;

    INSERT INTO core.Pagamento
    (
        ParcelaID,
        ValorPagamento,
        MeioPagamento,
        GatewayStatus,
        ReferenciaExterna,
        UsuarioLancamento,
        Observacao
    )
    VALUES
    (
        @ParcelaID,
        @ValorPagamento,
        @MeioPagamento,
        'CONFIRMED',
        @ReferenciaExterna,
        @UsuarioLancamento,
        @Observacao
    );

    ;WITH SomaPagamentos AS
    (
        SELECT
            ParcelaID,
            SUM(ValorPagamento) AS TotalPago,
            MAX(DataPagamento) AS UltimoPagamento
        FROM core.Pagamento
        WHERE ParcelaID = @ParcelaID
        GROUP BY ParcelaID
    )
    UPDATE pf
    SET
        ValorPago = sp.TotalPago,
        DataPagamento = CONVERT(DATE, sp.UltimoPagamento),
        StatusParcela = CASE
            WHEN sp.TotalPago >= pf.ValorLiquido THEN 'PAGA'
            WHEN pf.DataVencimento < CONVERT(DATE, SYSDATETIME()) THEN 'ATRASADA'
            ELSE 'ABERTA'
        END
    FROM core.ParcelaFinanceira pf
    JOIN SomaPagamentos sp ON sp.ParcelaID = pf.ParcelaID;

    DECLARE @AlunoID INT;
    SELECT @AlunoID = AlunoID FROM core.ParcelaFinanceira WHERE ParcelaID = @ParcelaID;

    DECLARE @EvtSuccess INT;
    SELECT @EvtSuccess = EventTypeID FROM telemetry.EventType WHERE EventCode = 'PAYMENT_SUCCESS';

    IF @EvtSuccess IS NOT NULL
    BEGIN
        INSERT INTO telemetry.EventStream
        (
            EventTypeID,
            AlunoID,
            SourceSystem,
            EventKey,
            NumericValue,
            PayloadJson
        )
        VALUES
        (
            @EvtSuccess,
            @AlunoID,
            'CORE.SP_REGISTRARPAGAMENTO',
            CONCAT('parcela-', @ParcelaID),
            @ValorPagamento,
            CONCAT(N'{"parcelaId":', @ParcelaID, N',"meio":"', @MeioPagamento, N'"}')
        );
    END;

    SELECT *
    FROM core.ParcelaFinanceira
    WHERE ParcelaID = @ParcelaID;
END;
GO

/* =============== PROCEDURE ANALYTICS =============== */
CREATE OR ALTER PROCEDURE analytics.sp_RebuildWarehouse
    @DataInicial DATE = NULL,
    @DataFinal DATE = NULL,
    @FullReload BIT = 1
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @RunID BIGINT;
    DECLARE @CorrelationID UNIQUEIDENTIFIER = NEWID();
    DECLARE @RowsDim INT = 0;
    DECLARE @RowsFact INT = 0;

    IF @DataInicial IS NULL
    BEGIN
        SELECT @DataInicial = MIN(CONVERT(DATE, DataMatricula)) FROM core.Matricula;
        IF @DataInicial IS NULL SET @DataInicial = CONVERT(DATE, SYSDATETIME());
    END;

    IF @DataFinal IS NULL
    BEGIN
        SELECT @DataFinal = CONVERT(DATE, SYSDATETIME());
    END;

    INSERT INTO log.ProcessRun (ProcessoNome, Camada, StatusExecucao, Mensagem, CorrelationID)
    VALUES ('analytics.sp_RebuildWarehouse', 'ANALYTICS', 'STARTED', N'Warehouse refresh iniciado.', @CorrelationID);

    SET @RunID = SCOPE_IDENTITY();

    BEGIN TRY
        ;WITH Datas AS
        (
            SELECT @DataInicial AS DataRef
            UNION ALL
            SELECT DATEADD(DAY, 1, DataRef)
            FROM Datas
            WHERE DataRef < @DataFinal
        )
        INSERT INTO analytics.DimTempo
        (
            TempoKey,
            DataCompleta,
            Ano,
            Mes,
            Dia,
            Trimestre,
            SemanaAno,
            DiaSemana,
            NomeDia,
            NomeMes,
            IsFimSemana
        )
        SELECT
            analytics.fn_TempoKey(DataRef),
            DataRef,
            DATEPART(YEAR, DataRef),
            DATEPART(MONTH, DataRef),
            DATEPART(DAY, DataRef),
            DATEPART(QUARTER, DataRef),
            DATEPART(WEEK, DataRef),
            DATEPART(WEEKDAY, DataRef),
            DATENAME(WEEKDAY, DataRef),
            DATENAME(MONTH, DataRef),
            CASE WHEN DATEPART(WEEKDAY, DataRef) IN (1,7) THEN 1 ELSE 0 END
        FROM Datas d
        WHERE NOT EXISTS
        (
            SELECT 1
            FROM analytics.DimTempo dt
            WHERE dt.DataCompleta = d.DataRef
        )
        OPTION (MAXRECURSION 0);

        IF @FullReload = 1
        BEGIN
            DELETE FROM analytics.FatoTelemetria;
            DELETE FROM analytics.FatoFinanceiro;
            DELETE FROM analytics.FatoAcademico;
            DELETE FROM analytics.KPI_Diario;
            DELETE FROM analytics.DimTurma;
            DELETE FROM analytics.DimAluno;
            DELETE FROM analytics.DimCurso;
        END;

        INSERT INTO analytics.DimCurso
        (
            CursoID,
            CodigoCurso,
            NomeCurso,
            DepartamentoSigla,
            Nivel,
            Modalidade,
            IsCurrent,
            EffectiveStartDate,
            EffectiveEndDate
        )
        SELECT
            c.CursoID,
            c.Codigo,
            c.Nome,
            d.Sigla,
            c.Nivel,
            c.Modalidade,
            1,
            CONVERT(DATE, SYSDATETIME()),
            NULL
        FROM core.Curso c
        JOIN core.Departamento d ON d.DepartamentoID = c.DepartamentoID;

        SET @RowsDim += @@ROWCOUNT;

        INSERT INTO analytics.DimAluno
        (
            AlunoID,
            RA,
            StatusAcademico,
            Bolsista,
            FaixaBolsa,
            IsCurrent,
            EffectiveStartDate,
            EffectiveEndDate
        )
        SELECT
            a.AlunoID,
            a.RA,
            a.StatusAcademico,
            a.Bolsista,
            CASE
                WHEN a.PercentualBolsa = 0 THEN 'SEM_BOLSA'
                WHEN a.PercentualBolsa < 50 THEN 'PARCIAL'
                WHEN a.PercentualBolsa < 100 THEN 'ALTA'
                ELSE 'INTEGRAL'
            END,
            1,
            CONVERT(DATE, SYSDATETIME()),
            NULL
        FROM core.Aluno a;

        SET @RowsDim += @@ROWCOUNT;

        INSERT INTO analytics.DimTurma
        (
            TurmaID,
            CodigoTurma,
            Ano,
            Periodo,
            Turno,
            DisciplinaCodigo,
            DisciplinaNome,
            CursoID,
            ProfessorID,
            ProfessorNome,
            Encerrada
        )
        SELECT
            t.TurmaID,
            t.CodigoTurma,
            t.Ano,
            t.Periodo,
            t.Turno,
            d.Codigo,
            d.Nome,
            d.CursoID,
            p.ProfessorID,
            ps.NomeExibicao,
            t.Encerrada
        FROM core.Turma t
        JOIN core.Disciplina d ON d.DisciplinaID = t.DisciplinaID
        JOIN core.Professor p ON p.ProfessorID = t.ProfessorID
        JOIN core.Pessoa ps ON ps.PessoaID = p.PessoaID;

        SET @RowsDim += @@ROWCOUNT;

        INSERT INTO analytics.FatoAcademico
        (
            TempoKey,
            AlunoKey,
            CursoKey,
            TurmaKey,
            MatriculaID,
            StatusMatricula,
            FrequenciaPercentual,
            NotaFinal,
            Aprovado,
            CargaHoraria,
            Creditos
        )
        SELECT
            analytics.fn_TempoKey(CONVERT(DATE, m.DataMatricula)) AS TempoKey,
            da.AlunoKey,
            dc.CursoKey,
            dt.TurmaKey,
            m.MatriculaID,
            m.StatusMatricula,
            m.FrequenciaPercentual,
            m.NotaFinal,
            CASE WHEN m.StatusMatricula = 'APROVADA' THEN 1 WHEN m.StatusMatricula = 'REPROVADA' THEN 0 ELSE NULL END,
            dis.CargaHoraria,
            dis.Creditos
        FROM core.Matricula m
        JOIN core.Turma t ON t.TurmaID = m.TurmaID
        JOIN core.Disciplina dis ON dis.DisciplinaID = t.DisciplinaID
        JOIN core.Curso c ON c.CursoID = dis.CursoID
        JOIN analytics.DimAluno da ON da.AlunoID = m.AlunoID AND da.IsCurrent = 1
        JOIN analytics.DimCurso dc ON dc.CursoID = c.CursoID AND dc.IsCurrent = 1
        JOIN analytics.DimTurma dt ON dt.TurmaID = t.TurmaID
        WHERE CONVERT(DATE, m.DataMatricula) BETWEEN @DataInicial AND @DataFinal;

        SET @RowsFact += @@ROWCOUNT;

        INSERT INTO analytics.FatoFinanceiro
        (
            TempoKey,
            AlunoKey,
            CursoKey,
            ParcelaID,
            Competencia,
            StatusParcela,
            ValorLiquido,
            ValorPago,
            SaldoAberto,
            AtrasoDias
        )
        SELECT
            analytics.fn_TempoKey(pf.DataVencimento),
            da.AlunoKey,
            dc.CursoKey,
            pf.ParcelaID,
            pf.Competencia,
            pf.StatusParcela,
            pf.ValorLiquido,
            ISNULL(pf.ValorPago, 0),
            CAST(pf.ValorLiquido - ISNULL(pf.ValorPago, 0) AS DECIMAL(12,2)),
            CASE
                WHEN pf.StatusParcela IN ('ABERTA', 'ATRASADA', 'NEGOCIADA')
                     AND pf.DataVencimento < CONVERT(DATE, SYSDATETIME())
                THEN DATEDIFF(DAY, pf.DataVencimento, CONVERT(DATE, SYSDATETIME()))
                ELSE 0
            END
        FROM core.ParcelaFinanceira pf
        JOIN analytics.DimAluno da ON da.AlunoID = pf.AlunoID AND da.IsCurrent = 1
        OUTER APPLY
        (
            SELECT TOP (1) c.CursoID
            FROM core.Matricula m
            JOIN core.Turma t ON t.TurmaID = m.TurmaID
            JOIN core.Disciplina d ON d.DisciplinaID = t.DisciplinaID
            JOIN core.Curso c ON c.CursoID = d.CursoID
            WHERE m.AlunoID = pf.AlunoID
            ORDER BY m.DataMatricula DESC
        ) x
        LEFT JOIN analytics.DimCurso dc ON dc.CursoID = x.CursoID AND dc.IsCurrent = 1
        WHERE pf.DataVencimento BETWEEN @DataInicial AND @DataFinal;

        SET @RowsFact += @@ROWCOUNT;

        INSERT INTO analytics.FatoTelemetria
        (
            TempoKey,
            EventTypeID,
            AlunoKey,
            SessionID,
            Eventos,
            DuracaoMediaMs,
            ValorNumericoSoma
        )
        SELECT
            analytics.fn_TempoKey(CONVERT(DATE, e.OccurredAt)) AS TempoKey,
            e.EventTypeID,
            da.AlunoKey,
            e.SessionID,
            COUNT(*) AS Eventos,
            CAST(AVG(CAST(e.DurationMs AS DECIMAL(18,2))) AS DECIMAL(18,2)) AS DuracaoMediaMs,
            CAST(SUM(ISNULL(e.NumericValue, 0)) AS DECIMAL(18,4)) AS ValorNumericoSoma
        FROM telemetry.EventStream e
        LEFT JOIN analytics.DimAluno da ON da.AlunoID = e.AlunoID AND da.IsCurrent = 1
        WHERE CONVERT(DATE, e.OccurredAt) BETWEEN @DataInicial AND @DataFinal
        GROUP BY
            analytics.fn_TempoKey(CONVERT(DATE, e.OccurredAt)),
            e.EventTypeID,
            da.AlunoKey,
            e.SessionID;

        SET @RowsFact += @@ROWCOUNT;

        DELETE FROM analytics.KPI_Diario
        WHERE DataRef BETWEEN @DataInicial AND @DataFinal;

        INSERT INTO analytics.KPI_Diario (DataRef, KPIName, KPIValue, Unidade, MetaValor)
        SELECT dt.DataCompleta, 'ALUNOS_ATIVOS', COUNT(DISTINCT fa.AlunoKey), 'QTD', 100
        FROM analytics.DimTempo dt
        LEFT JOIN analytics.FatoAcademico fa ON fa.TempoKey = dt.TempoKey
        WHERE dt.DataCompleta BETWEEN @DataInicial AND @DataFinal
        GROUP BY dt.DataCompleta;

        INSERT INTO analytics.KPI_Diario (DataRef, KPIName, KPIValue, Unidade, MetaValor)
        SELECT
            dt.DataCompleta,
            'TAXA_APROVACAO',
            CAST(
                100.0 * SUM(CASE WHEN fa.StatusMatricula = 'APROVADA' THEN 1 ELSE 0 END)
                / NULLIF(SUM(CASE WHEN fa.StatusMatricula IN ('APROVADA','REPROVADA') THEN 1 ELSE 0 END), 0)
                AS DECIMAL(18,4)
            ),
            'PCT',
            85
        FROM analytics.DimTempo dt
        LEFT JOIN analytics.FatoAcademico fa ON fa.TempoKey = dt.TempoKey
        WHERE dt.DataCompleta BETWEEN @DataInicial AND @DataFinal
        GROUP BY dt.DataCompleta;

        INSERT INTO analytics.KPI_Diario (DataRef, KPIName, KPIValue, Unidade, MetaValor)
        SELECT dt.DataCompleta, 'INADIMPLENCIA_VALOR', CAST(SUM(ISNULL(ff.SaldoAberto, 0)) AS DECIMAL(18,4)), 'BRL', 0
        FROM analytics.DimTempo dt
        LEFT JOIN analytics.FatoFinanceiro ff ON ff.TempoKey = dt.TempoKey
        WHERE dt.DataCompleta BETWEEN @DataInicial AND @DataFinal
        GROUP BY dt.DataCompleta;

        INSERT INTO analytics.KPI_Diario (DataRef, KPIName, KPIValue, Unidade, MetaValor)
        SELECT dt.DataCompleta, 'EVENTOS_TOTAL', CAST(SUM(ISNULL(ft.Eventos, 0)) AS DECIMAL(18,4)), 'QTD', 0
        FROM analytics.DimTempo dt
        LEFT JOIN analytics.FatoTelemetria ft ON ft.TempoKey = dt.TempoKey
        WHERE dt.DataCompleta BETWEEN @DataInicial AND @DataFinal
        GROUP BY dt.DataCompleta;

        UPDATE log.ProcessRun
        SET
            FinalizadoEm = SYSDATETIME(),
            StatusExecucao = 'SUCCESS',
            LinhasProcessadas = @RowsDim + @RowsFact,
            Mensagem = CONCAT(N'Warehouse refresh concluido. Dim=', @RowsDim, N' Fact=', @RowsFact)
        WHERE ProcessRunID = @RunID;
    END TRY
    BEGIN CATCH
        UPDATE log.ProcessRun
        SET
            FinalizadoEm = SYSDATETIME(),
            StatusExecucao = 'FAILED',
            Mensagem = ERROR_MESSAGE()
        WHERE ProcessRunID = @RunID;

        DECLARE @ErrNum INT = ERROR_NUMBER();
        DECLARE @ErrSeverity SMALLINT = ERROR_SEVERITY();
        DECLARE @ErrState SMALLINT = ERROR_STATE();
        DECLARE @ErrProc SYSNAME = ERROR_PROCEDURE();
        DECLARE @ErrLine INT = ERROR_LINE();
        DECLARE @ErrMsg NVARCHAR(MAX) = ERROR_MESSAGE();
        DECLARE @ErrPayload NVARCHAR(MAX) =
            CONCAT(N'{"dataInicial":"', CONVERT(VARCHAR(10), @DataInicial, 120), N'","dataFinal":"', CONVERT(VARCHAR(10), @DataFinal, 120), N'"}');

        EXEC log.sp_RegisterError
            @ProcessoNome = 'analytics.sp_RebuildWarehouse',
            @Etapa = 'CATCH',
            @NumeroErro = @ErrNum,
            @Severidade = @ErrSeverity,
            @Estado = @ErrState,
            @ProcedureName = @ErrProc,
            @Linha = @ErrLine,
            @Mensagem = @ErrMsg,
            @Payload = @ErrPayload,
            @CorrelationID = @CorrelationID;

        THROW;
    END CATCH;
END;
GO

CREATE OR ALTER PROCEDURE telemetry.sp_RebuildMetricMinute
    @DataInicial DATETIME2(0) = NULL,
    @DataFinal DATETIME2(0) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF @DataInicial IS NULL SET @DataInicial = DATEADD(DAY, -7, SYSDATETIME());
    IF @DataFinal IS NULL SET @DataFinal = SYSDATETIME();

    DELETE FROM telemetry.MetricMinute
    WHERE MetricMinute BETWEEN DATEADD(MINUTE, DATEDIFF(MINUTE, 0, @DataInicial), 0)
                          AND DATEADD(MINUTE, DATEDIFF(MINUTE, 0, @DataFinal), 0);

    INSERT INTO telemetry.MetricMinute
    (
        MetricName,
        MetricMinute,
        Dimension1,
        Dimension2,
        MetricValue
    )
    SELECT
        'EVENT_COUNT',
        DATEADD(MINUTE, DATEDIFF(MINUTE, 0, e.OccurredAt), 0) AS MetricMinute,
        et.EventCode,
        e.SourceSystem,
        COUNT(*)
    FROM telemetry.EventStream e
    JOIN telemetry.EventType et ON et.EventTypeID = e.EventTypeID
    WHERE e.OccurredAt BETWEEN @DataInicial AND @DataFinal
    GROUP BY
        DATEADD(MINUTE, DATEDIFF(MINUTE, 0, e.OccurredAt), 0),
        et.EventCode,
        e.SourceSystem;
END;
GO
/* =============== VIEWS =============== */
CREATE OR ALTER VIEW analytics.vw_RankingTurma
AS
SELECT
    t.TurmaID,
    t.CodigoTurma,
    m.MatriculaID,
    a.AlunoID,
    a.RA,
    p.NomeExibicao AS NomeAluno,
    m.NotaFinal,
    m.FrequenciaPercentual,
    m.StatusMatricula,
    ROW_NUMBER() OVER (PARTITION BY t.TurmaID ORDER BY m.NotaFinal DESC, p.NomeExibicao ASC) AS RowNumberTurma,
    RANK() OVER (PARTITION BY t.TurmaID ORDER BY m.NotaFinal DESC) AS RankTurma,
    DENSE_RANK() OVER (PARTITION BY t.TurmaID ORDER BY m.NotaFinal DESC) AS DenseRankTurma,
    NTILE(4) OVER (PARTITION BY t.TurmaID ORDER BY m.NotaFinal DESC) AS Quartil,
    LAG(m.NotaFinal) OVER (PARTITION BY t.TurmaID ORDER BY m.NotaFinal DESC, p.NomeExibicao ASC) AS NotaAnterior,
    LEAD(m.NotaFinal) OVER (PARTITION BY t.TurmaID ORDER BY m.NotaFinal DESC, p.NomeExibicao ASC) AS NotaSeguinte
FROM core.Matricula m
JOIN core.Turma t ON t.TurmaID = m.TurmaID
JOIN core.Aluno a ON a.AlunoID = m.AlunoID
JOIN core.Pessoa p ON p.PessoaID = a.PessoaID
WHERE m.NotaFinal IS NOT NULL;
GO

CREATE OR ALTER VIEW analytics.vw_VisaoUnicaFaculda
AS
SELECT
    a.AlunoID,
    a.RA,
    p.NomeExibicao,
    p.Email,
    a.StatusAcademico,
    c.Codigo AS CursoCodigo,
    c.Nome AS CursoNome,
    core.fn_CRA_Aluno(a.AlunoID) AS CRA,
    fs.ParcelasPendentes,
    fs.ValorEmAberto,
    fs.ValorPago,
    acad.TotalMatriculas,
    acad.Aprovacoes,
    acad.Reprovacoes,
    tel.Eventos30Dias,
    tel.UltimoEventoEm,
    ult.CodigoTurma AS UltimaTurma,
    ult.StatusMatricula AS UltimoStatusMatricula,
    ult.DataMatricula AS UltimaMovimentacao
FROM core.Aluno a
JOIN core.Pessoa p ON p.PessoaID = a.PessoaID
OUTER APPLY
(
    SELECT TOP (1)
        c1.CursoID,
        c1.Codigo,
        c1.Nome,
        COUNT(*) AS Cnt
    FROM core.Matricula m
    JOIN core.Turma t ON t.TurmaID = m.TurmaID
    JOIN core.Disciplina d ON d.DisciplinaID = t.DisciplinaID
    JOIN core.Curso c1 ON c1.CursoID = d.CursoID
    WHERE m.AlunoID = a.AlunoID
    GROUP BY c1.CursoID, c1.Codigo, c1.Nome
    ORDER BY COUNT(*) DESC, c1.Nome ASC
) c
OUTER APPLY core.fn_SaldoAluno(a.AlunoID) fs
OUTER APPLY
(
    SELECT
        COUNT(*) AS TotalMatriculas,
        SUM(CASE WHEN m.StatusMatricula = 'APROVADA' THEN 1 ELSE 0 END) AS Aprovacoes,
        SUM(CASE WHEN m.StatusMatricula = 'REPROVADA' THEN 1 ELSE 0 END) AS Reprovacoes
    FROM core.Matricula m
    WHERE m.AlunoID = a.AlunoID
) acad
OUTER APPLY
(
    SELECT
        COUNT(*) AS Eventos30Dias,
        MAX(e.OccurredAt) AS UltimoEventoEm
    FROM telemetry.EventStream e
    WHERE
        e.AlunoID = a.AlunoID
        AND e.OccurredAt >= DATEADD(DAY, -30, SYSDATETIME())
) tel
OUTER APPLY
(
    SELECT TOP (1)
        t.CodigoTurma,
        m.StatusMatricula,
        m.DataMatricula
    FROM core.Matricula m
    JOIN core.Turma t ON t.TurmaID = m.TurmaID
    WHERE m.AlunoID = a.AlunoID
    ORDER BY m.DataMatricula DESC
) ult;
GO

CREATE OR ALTER VIEW analytics.vw_PainelExecutivo
AS
SELECT
    c.Codigo AS CursoCodigo,
    c.Nome AS CursoNome,
    COUNT(DISTINCT a.AlunoID) AS Alunos,
    COUNT(DISTINCT t.TurmaID) AS Turmas,
    CAST(AVG(CAST(m.NotaFinal AS DECIMAL(10,4))) AS DECIMAL(10,2)) AS MediaNotas,
    CAST(SUM(CASE WHEN pf.StatusParcela IN ('ABERTA', 'ATRASADA', 'NEGOCIADA') THEN pf.ValorLiquido - ISNULL(pf.ValorPago, 0) ELSE 0 END) AS DECIMAL(14,2)) AS Inadimplencia,
    CAST(SUM(ISNULL(pf.ValorPago, 0)) AS DECIMAL(14,2)) AS ReceitaRealizada,
    CAST(SUM(CASE WHEN m.StatusMatricula = 'APROVADA' THEN 1 ELSE 0 END) AS DECIMAL(14,2)) AS MatriculasAprovadas
FROM core.Curso c
LEFT JOIN core.Disciplina d ON d.CursoID = c.CursoID
LEFT JOIN core.Turma t ON t.DisciplinaID = d.DisciplinaID
LEFT JOIN core.Matricula m ON m.TurmaID = t.TurmaID
LEFT JOIN core.Aluno a ON a.AlunoID = m.AlunoID
LEFT JOIN core.ParcelaFinanceira pf ON pf.AlunoID = a.AlunoID
GROUP BY c.Codigo, c.Nome;
GO

CREATE OR ALTER VIEW telemetry.vw_EventHealth
AS
SELECT
    et.EventCode,
    e.SourceSystem,
    COUNT(*) AS Eventos,
    CAST(AVG(CAST(e.DurationMs AS DECIMAL(18,2))) AS DECIMAL(18,2)) AS AvgDurationMs,
    MAX(e.OccurredAt) AS UltimoEvento,
    SUM(CASE WHEN e.ProcessingStatus = 'E' THEN 1 ELSE 0 END) AS EventosComErro
FROM telemetry.EventStream e
JOIN telemetry.EventType et ON et.EventTypeID = e.EventTypeID
GROUP BY et.EventCode, e.SourceSystem;
GO

CREATE OR ALTER VIEW log.vw_AuditoriaRecente
AS
SELECT
    ca.AuditID,
    ca.EventTime,
    ca.OrigemSchema,
    ca.OrigemObjeto,
    ca.Operacao,
    ca.ChaveRegistro,
    ca.AppUser,
    ca.HostName,
    ca.CorrelationID
FROM log.ChangeAudit ca
WHERE ca.EventTime >= DATEADD(DAY, -15, SYSDATETIME());
GO

CREATE OR ALTER VIEW dbo.vw_VisaoUnicaFaculda
AS
SELECT * FROM analytics.vw_VisaoUnicaFaculda;
GO

/* =============== TRIGGERS =============== */
CREATE OR ALTER TRIGGER core.trg_Nota_ValidateAndDerive
ON core.Nota
AFTER INSERT, UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    IF EXISTS
    (
        SELECT 1
        FROM inserted i
        JOIN core.Avaliacao av ON av.AvaliacaoID = i.AvaliacaoID
        WHERE i.NotaObtida > av.NotaMaxima
    )
    BEGIN
        THROW 51100, N'Nota obtida nao pode ser maior que NotaMaxima.', 1;
    END;

    ;WITH Calc AS
    (
        SELECT
            n.MatriculaID,
            CAST(ROUND(SUM(n.NotaObtida * av.Peso) / NULLIF(SUM(av.Peso), 0), 2) AS DECIMAL(5,2)) AS NotaFinalCalculada
        FROM core.Nota n
        JOIN core.Avaliacao av ON av.AvaliacaoID = n.AvaliacaoID
        WHERE n.MatriculaID IN (SELECT DISTINCT MatriculaID FROM inserted)
        GROUP BY n.MatriculaID
    )
    UPDATE m
    SET
        NotaFinal = c.NotaFinalCalculada,
        StatusMatricula = CASE
            WHEN m.FrequenciaPercentual >= 75 AND c.NotaFinalCalculada >= 6 THEN 'APROVADA'
            WHEN m.FrequenciaPercentual < 75 OR c.NotaFinalCalculada < 6 THEN 'REPROVADA'
            ELSE m.StatusMatricula
        END,
        UltimaAtualizacao = SYSDATETIME()
    FROM core.Matricula m
    JOIN Calc c ON c.MatriculaID = m.MatriculaID
    WHERE m.StatusMatricula NOT IN ('CANCELADA', 'TRANCADA');

    INSERT INTO log.ChangeAudit
    (
        OrigemSchema,
        OrigemObjeto,
        ChaveRegistro,
        Operacao,
        DadosAntes,
        DadosDepois
    )
    SELECT
        'core',
        'Nota',
        CONVERT(NVARCHAR(200), i.NotaID),
        CASE WHEN d.NotaID IS NULL THEN 'INSERT' ELSE 'UPDATE' END,
        CASE
            WHEN d.NotaID IS NULL THEN NULL
            ELSE CONCAT(N'Nota=', CONVERT(NVARCHAR(30), d.NotaObtida), N'; AvaliacaoID=', d.AvaliacaoID)
        END,
        CONCAT(N'Nota=', CONVERT(NVARCHAR(30), i.NotaObtida), N'; AvaliacaoID=', i.AvaliacaoID)
    FROM inserted i
    LEFT JOIN deleted d ON d.NotaID = i.NotaID;
END;
GO

CREATE OR ALTER TRIGGER core.trg_Matricula_Audit
ON core.Matricula
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    IF NOT (UPDATE(StatusMatricula) OR UPDATE(FrequenciaPercentual) OR UPDATE(NotaFinal) OR UPDATE(TurmaID))
    BEGIN
        RETURN;
    END;

    INSERT INTO log.ChangeAudit
    (
        OrigemSchema,
        OrigemObjeto,
        ChaveRegistro,
        Operacao,
        DadosAntes,
        DadosDepois
    )
    SELECT
        'core',
        'Matricula',
        CONVERT(NVARCHAR(200), i.MatriculaID),
        'UPDATE',
        CONCAT(
            N'Status=', ISNULL(d.StatusMatricula, N'NULL'),
            N'; Freq=', ISNULL(CONVERT(NVARCHAR(30), d.FrequenciaPercentual), N'NULL'),
            N'; Nota=', ISNULL(CONVERT(NVARCHAR(30), d.NotaFinal), N'NULL'),
            N'; Turma=', ISNULL(CONVERT(NVARCHAR(30), d.TurmaID), N'NULL')
        ),
        CONCAT(
            N'Status=', ISNULL(i.StatusMatricula, N'NULL'),
            N'; Freq=', ISNULL(CONVERT(NVARCHAR(30), i.FrequenciaPercentual), N'NULL'),
            N'; Nota=', ISNULL(CONVERT(NVARCHAR(30), i.NotaFinal), N'NULL'),
            N'; Turma=', ISNULL(CONVERT(NVARCHAR(30), i.TurmaID), N'NULL')
        )
    FROM inserted i
    JOIN deleted d ON d.MatriculaID = i.MatriculaID
    WHERE
        ISNULL(i.StatusMatricula, '') <> ISNULL(d.StatusMatricula, '')
        OR ISNULL(i.FrequenciaPercentual, -1) <> ISNULL(d.FrequenciaPercentual, -1)
        OR ISNULL(i.NotaFinal, -1) <> ISNULL(d.NotaFinal, -1)
        OR ISNULL(i.TurmaID, -1) <> ISNULL(d.TurmaID, -1);
END;
GO

CREATE OR ALTER TRIGGER core.trg_Pagamento_RollupAudit
ON core.Pagamento
AFTER INSERT
AS
BEGIN
    SET NOCOUNT ON;

    ;WITH Afetadas AS
    (
        SELECT DISTINCT ParcelaID FROM inserted
    ),
    Soma AS
    (
        SELECT
            p.ParcelaID,
            SUM(p.ValorPagamento) AS TotalPago,
            MAX(p.DataPagamento) AS UltimoPagamento
        FROM core.Pagamento p
        JOIN Afetadas a ON a.ParcelaID = p.ParcelaID
        GROUP BY p.ParcelaID
    )
    UPDATE pf
    SET
        ValorPago = s.TotalPago,
        DataPagamento = CONVERT(DATE, s.UltimoPagamento),
        StatusParcela = CASE
            WHEN s.TotalPago >= pf.ValorLiquido THEN 'PAGA'
            WHEN pf.DataVencimento < CONVERT(DATE, SYSDATETIME()) THEN 'ATRASADA'
            ELSE 'ABERTA'
        END
    FROM core.ParcelaFinanceira pf
    JOIN Soma s ON s.ParcelaID = pf.ParcelaID;

    INSERT INTO log.ChangeAudit
    (
        OrigemSchema,
        OrigemObjeto,
        ChaveRegistro,
        Operacao,
        DadosAntes,
        DadosDepois
    )
    SELECT
        'core',
        'Pagamento',
        CONVERT(NVARCHAR(200), i.PagamentoID),
        'INSERT',
        NULL,
        CONCAT(
            N'Parcela=', i.ParcelaID,
            N'; Valor=', i.ValorPagamento,
            N'; Meio=', i.MeioPagamento,
            N'; Gateway=', i.GatewayStatus
        )
    FROM inserted i;
END;
GO

CREATE OR ALTER TRIGGER telemetry.trg_EventStream_MinuteAgg
ON telemetry.EventStream
AFTER INSERT
AS
BEGIN
    SET NOCOUNT ON;

    ;WITH Agg AS
    (
        SELECT
            'EVENT_COUNT' AS MetricName,
            DATEADD(MINUTE, DATEDIFF(MINUTE, 0, i.OccurredAt), 0) AS MetricMinute,
            et.EventCode AS Dimension1,
            i.SourceSystem AS Dimension2,
            CAST(COUNT(*) AS DECIMAL(18,4)) AS MetricValue
        FROM inserted i
        JOIN telemetry.EventType et ON et.EventTypeID = i.EventTypeID
        GROUP BY
            DATEADD(MINUTE, DATEDIFF(MINUTE, 0, i.OccurredAt), 0),
            et.EventCode,
            i.SourceSystem
    )
    MERGE telemetry.MetricMinute AS tgt
    USING Agg AS src
    ON
       tgt.MetricName = src.MetricName
       AND tgt.MetricMinute = src.MetricMinute
       AND ISNULL(tgt.Dimension1, '') = ISNULL(src.Dimension1, '')
       AND ISNULL(tgt.Dimension2, '') = ISNULL(src.Dimension2, '')
    WHEN MATCHED THEN
        UPDATE SET tgt.MetricValue = tgt.MetricValue + src.MetricValue
    WHEN NOT MATCHED THEN
        INSERT (MetricName, MetricMinute, Dimension1, Dimension2, MetricValue)
        VALUES (src.MetricName, src.MetricMinute, src.Dimension1, src.Dimension2, src.MetricValue);
END;
GO

/* =============== DCL =============== */
IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'rl_core_ops')
BEGIN
    CREATE ROLE rl_core_ops AUTHORIZATION dbo;
END;

IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'rl_finance_ops')
BEGIN
    CREATE ROLE rl_finance_ops AUTHORIZATION dbo;
END;

IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'rl_analytics_ops')
BEGIN
    CREATE ROLE rl_analytics_ops AUTHORIZATION dbo;
END;

IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'rl_observer')
BEGIN
    CREATE ROLE rl_observer AUTHORIZATION dbo;
END;
GO

GRANT SELECT, INSERT, UPDATE ON SCHEMA::core TO rl_core_ops;
GRANT SELECT, INSERT ON SCHEMA::telemetry TO rl_core_ops;
GRANT SELECT ON SCHEMA::log TO rl_core_ops;
GRANT EXECUTE ON SCHEMA::core TO rl_core_ops;
GRANT EXECUTE ON OBJECT::telemetry.sp_StartSession TO rl_core_ops;
GRANT EXECUTE ON OBJECT::telemetry.sp_IngerirEvento TO rl_core_ops;
GO

GRANT SELECT ON SCHEMA::core TO rl_finance_ops;
GRANT SELECT ON SCHEMA::analytics TO rl_finance_ops;
GRANT SELECT, INSERT, UPDATE ON OBJECT::core.Pagamento TO rl_finance_ops;
GRANT SELECT, INSERT, UPDATE ON OBJECT::core.ParcelaFinanceira TO rl_finance_ops;
GRANT EXECUTE ON OBJECT::core.sp_RegistrarPagamento TO rl_finance_ops;
GO

GRANT SELECT ON SCHEMA::core TO rl_analytics_ops;
GRANT SELECT ON SCHEMA::telemetry TO rl_analytics_ops;
GRANT SELECT ON SCHEMA::log TO rl_analytics_ops;
GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA::analytics TO rl_analytics_ops;
GRANT EXECUTE ON SCHEMA::analytics TO rl_analytics_ops;
GRANT EXECUTE ON OBJECT::telemetry.sp_RebuildMetricMinute TO rl_analytics_ops;
GO

GRANT SELECT ON OBJECT::analytics.vw_VisaoUnicaFaculda TO rl_observer;
GRANT SELECT ON OBJECT::analytics.vw_PainelExecutivo TO rl_observer;
GRANT SELECT ON OBJECT::analytics.vw_RankingTurma TO rl_observer;
GRANT SELECT ON OBJECT::telemetry.vw_EventHealth TO rl_observer;
GRANT SELECT ON OBJECT::log.vw_AuditoriaRecente TO rl_observer;
GO

/* =============== BOOTSTRAP ANALYTICS =============== */
EXEC analytics.sp_RebuildWarehouse @FullReload = 1;
EXEC telemetry.sp_RebuildMetricMinute;
GO

PRINT N'Complementacao enterprise aplicada com sucesso.';
GO

/* Consultas demonstracao */
SELECT TOP (20)
    v.AlunoID,
    v.NomeExibicao,
    v.CursoNome,
    v.CRA,
    v.ValorEmAberto,
    v.Eventos30Dias
FROM analytics.vw_VisaoUnicaFaculda v
ORDER BY v.ValorEmAberto DESC, v.CRA DESC, v.NomeExibicao;
GO

SELECT TOP (20)
    p.CursoCodigo,
    p.CursoNome,
    p.Alunos,
    p.Inadimplencia,
    p.ReceitaRealizada,
    p.MediaNotas
FROM analytics.vw_PainelExecutivo p
ORDER BY p.Inadimplencia DESC, p.CursoNome;
GO

SELECT TOP (20)
    r.CodigoTurma,
    r.NomeAluno,
    r.NotaFinal,
    r.RankTurma,
    r.Quartil
FROM analytics.vw_RankingTurma r
ORDER BY r.CodigoTurma, r.RankTurma;
GO

