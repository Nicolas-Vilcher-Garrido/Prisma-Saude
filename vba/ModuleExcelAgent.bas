Option Explicit

Private Function PyExe() As String
    ' Ajuste aqui o caminho do seu Python (ou deixe "" para usar o do PATH)
    PyExe = "C:\Users\Nicolas\AppData\Local\Programs\Python\Python313\python.exe"
End Function

Private Function AgentPy() As String
    AgentPy = ThisWorkbook.Path & "\scripts\agent.py"
End Function

Private Sub RunAgent(ByVal arg As String)
    Dim cmd As String

    If Dir(AgentPy) = "" Then
        MsgBox "Script não encontrado: " & AgentPy, vbCritical
        Exit Sub
    End If

    If PyExe <> "" Then
        cmd = "cmd /c " & """" & PyExe & """" & " " & """" & AgentPy & """" & " " & arg
    Else
        cmd = "cmd /c python " & """" & AgentPy & """" & " " & arg
    End If

    Debug.Print cmd
    Shell cmd, vbNormalFocus
End Sub

Public Sub Botao_AtualizarTudo()
    RunAgent "atualizar_tudo"
End Sub

Public Sub Botao_GerarGraficos()
    RunAgent "gerar_graficos"
End Sub

Public Sub Botao_GerarRankings()
    RunAgent "gerar_rankings"
End Sub
