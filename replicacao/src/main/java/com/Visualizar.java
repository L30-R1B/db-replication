package com;

import java.awt.BorderLayout;
import java.awt.Font;
import java.awt.GridLayout;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTabbedPane;
import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.SwingUtilities;
import javax.swing.Timer;
import javax.swing.table.DefaultTableModel;

import io.github.cdimascio.dotenv.Dotenv;

public class Visualizar {
    private static String sqlServerUrl, sqlServerUser, sqlServerPassword;
    private static String mysqlUrl, mysqlUser, mysqlPassword;
    private static List<String[]> tablePairs;
    private static ExecutorService executorService; // Pool de threads para consultas ao banco de dados

    public static void main(String[] args) {
        carregarCredenciais();
        executorService = Executors.newFixedThreadPool(4); // Pool de 4 threads
        SwingUtilities.invokeLater(Visualizar::criarGUI);
    }

    private static void carregarCredenciais() {
        Dotenv dotenv = Dotenv.load();
        sqlServerUrl = dotenv.get("SQLSERVER_URL");
        sqlServerUser = dotenv.get("SQLSERVER_USER");
        sqlServerPassword = dotenv.get("SQLSERVER_PASSWORD");
        mysqlUrl = dotenv.get("MYSQL_URL");
        mysqlUser = dotenv.get("MYSQL_USER");
        mysqlPassword = dotenv.get("MYSQL_PASSWORD");

        System.out.println(mysqlUrl + "\n" + mysqlUser + "\n" +  mysqlPassword);

        // Obtém os pares de tabelas da variável de ambiente
        String tabelas = dotenv.get("PARES_TABELAS"); // Exemplo: "clientes_sql,clientes_mysql;pedidos_sql,pedidos_mysql"
        tablePairs = Arrays.stream(tabelas.split(";"))
                           .map(pair -> pair.split(","))
                           .toList();
    }

    private static void criarGUI() {
        JFrame frame = new JFrame("Visualização de Tabelas - SQL Server (Protheus) e MySQL (Cloud SQL)");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setExtendedState(JFrame.MAXIMIZED_BOTH);

        JTabbedPane tabbedPane = new JTabbedPane();
        frame.add(tabbedPane);

        for (String[] pair : tablePairs) {
            String sqlServerTable = pair[0];
            String mysqlTable = pair[1];

            JPanel panelTabela = new JPanel(new GridLayout(2, 1));

            JPanel panelSQLServer = criarPainelTabela("SQL Server (Protheus) - " + sqlServerTable);
            panelTabela.add(panelSQLServer);

            JPanel panelMySQL = criarPainelTabela("MySQL (Cloud SQL) - " + mysqlTable);
            panelTabela.add(panelMySQL);

            tabbedPane.addTab(sqlServerTable + " / " + mysqlTable, panelTabela);

            Timer timer = new Timer(5000, e -> {
                if (tabbedPane.getSelectedComponent() == panelTabela) {
                    atualizarTabela(panelSQLServer, sqlServerUrl, sqlServerUser, sqlServerPassword, sqlServerTable);
                    atualizarTabela(panelMySQL, mysqlUrl, mysqlUser, mysqlPassword, mysqlTable);
                }
            });
            timer.start();
        }

        // Adicionar painel de controle para inserção e remoção de dados
        JPanel panelControle = new JPanel(new GridLayout(1, 4));
        JTextField entradaK = new JTextField(5);
        JTextField entradaM = new JTextField(5);
        JTextField entradaS = new JTextField(5);
        JTextField entradaP = new JTextField(5);

        JButton botaoInserirSQLServer = new JButton("Inserir Dados SQL Server");
        botaoInserirSQLServer.addActionListener(e -> {
            int k = Integer.parseInt(entradaK.getText());
            int m = Integer.parseInt(entradaM.getText());
            int s = Integer.parseInt(entradaS.getText());
            inserirDadosAleatoriosSQLServer(k, m, s);
        });

        JButton botaoInserirMySQL = new JButton("Inserir Dados MySQL");
        botaoInserirMySQL.addActionListener(e -> {
            int k = Integer.parseInt(entradaK.getText());
            int m = Integer.parseInt(entradaM.getText());
            int s = Integer.parseInt(entradaS.getText());
            inserirDadosAleatoriosMySQL(k, m, s);
        });

        JButton botaoApagarSQLServer = new JButton("Apagar Linhas SQL Server");
        botaoApagarSQLServer.addActionListener(e -> {
            double p = Double.parseDouble(entradaP.getText());
            apagarLinhasComProbabilidadeSQLServer(p);
        });

        JButton botaoApagarMySQL = new JButton("Apagar Linhas MySQL");
        botaoApagarMySQL.addActionListener(e -> {
            double p = Double.parseDouble(entradaP.getText());
            apagarLinhasComProbabilidadeMySQL(p);
        });

        panelControle.add(new JLabel("K:"));
        panelControle.add(entradaK);
        panelControle.add(new JLabel("M:"));
        panelControle.add(entradaM);
        panelControle.add(new JLabel("S:"));
        panelControle.add(entradaS);
        panelControle.add(new JLabel("P:"));
        panelControle.add(entradaP);
        panelControle.add(botaoInserirSQLServer);
        panelControle.add(botaoInserirMySQL);
        panelControle.add(botaoApagarSQLServer);
        panelControle.add(botaoApagarMySQL);

        frame.add(panelControle, BorderLayout.SOUTH);
        frame.setVisible(true);
    }

    private static JPanel criarPainelTabela(String titulo) {
        JPanel panel = new JPanel(new BorderLayout());
        JLabel label = new JLabel(titulo, JLabel.CENTER);
        label.setFont(new Font("Arial", Font.BOLD, 24));
        panel.add(label, BorderLayout.NORTH);

        DefaultTableModel model = new DefaultTableModel();
        JTable table = new JTable(model);
        JScrollPane scrollPane = new JScrollPane(table);
        panel.add(scrollPane, BorderLayout.CENTER);

        return panel;
    }

    private static void atualizarTabela(JPanel panel, String url, String user, String password, String tableName) {
        executorService.submit(() -> {
            try (Connection conn = DriverManager.getConnection(url, user, password);
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {

                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();

                DefaultTableModel model = new DefaultTableModel();
                for (int i = 1; i <= columnCount; i++) {
                    model.addColumn(metaData.getColumnName(i));
                }

                while (rs.next()) {
                    Object[] row = new Object[columnCount];
                    for (int i = 0; i < columnCount; i++) {
                        row[i] = rs.getObject(i + 1);
                    }
                    model.addRow(row);
                }

                // Atualiza a tabela na thread da interface gráfica
                SwingUtilities.invokeLater(() -> {
                    JScrollPane scrollPane = (JScrollPane) panel.getComponent(1);
                    JTable table = (JTable) scrollPane.getViewport().getView();
                    table.setModel(model);

                    // Remove a mensagem de "Tabela não existe" se a tabela for carregada com sucesso
                    if (panel.getComponentCount() > 2) {
                        panel.remove(2);
                        panel.revalidate();
                        panel.repaint();
                    }
                });
            } catch (SQLException ex) {
                // Exibe "Tabela não existe" se a tabela não for encontrada
                if (ex.getMessage().contains("Invalid object name") || ex.getMessage().contains("Table") && ex.getMessage().contains("doesn't exist")) {
                    SwingUtilities.invokeLater(() -> {
                        JLabel labelErro = new JLabel("Tabela não existe", JLabel.CENTER);
                        labelErro.setFont(new Font("Arial", Font.BOLD, 36));
                        labelErro.setForeground(java.awt.Color.RED);

                        // Remove o JLabel anterior, se existir
                        if (panel.getComponentCount() > 2) {
                            panel.remove(2);
                        }

                        panel.add(labelErro, BorderLayout.SOUTH);
                        panel.revalidate();
                        panel.repaint();
                    });
                } else {
                    ex.printStackTrace();
                    System.err.println("Erro ao conectar ao banco de dados: " + ex.getMessage());
                }
            }
        });
    }

    private static void inserirDadosAleatoriosSQLServer(int k, int m, int s) {
        executorService.submit(() -> {
            try (Connection conn = DriverManager.getConnection(sqlServerUrl, sqlServerUser, sqlServerPassword)) {
                Random random = new Random();
                long tempoFinal = System.currentTimeMillis() + s * 1000;

                while (System.currentTimeMillis() < tempoFinal) {
                    for (int i = 0; i < k; i++) {
                        String sql = "INSERT INTO teste (coluna_int1, coluna_int2, coluna_float1, coluna_float2, coluna_float3, coluna_float4, coluna_float5, coluna_data, coluna_str1, coluna_str2, coluna_str3, coluna_str4, coluna_str5, coluna_str6, coluna_str7) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                            pstmt.setInt(1, random.nextInt(100));
                            pstmt.setInt(2, random.nextInt(100));
                            pstmt.setFloat(3, random.nextFloat() * 100);
                            pstmt.setFloat(4, random.nextFloat() * 100);
                            pstmt.setFloat(5, random.nextFloat() * 100);
                            pstmt.setFloat(6, random.nextFloat() * 100);
                            pstmt.setFloat(7, random.nextFloat() * 100);
                            pstmt.setDate(8, new java.sql.Date(System.currentTimeMillis()));
                            pstmt.setString(9, "Str" + random.nextInt(100));
                            pstmt.setString(10, "Str" + random.nextInt(100));
                            pstmt.setString(11, "Str" + random.nextInt(100));
                            pstmt.setString(12, "Str" + random.nextInt(100));
                            pstmt.setString(13, "Str" + random.nextInt(100));
                            pstmt.setString(14, "Str" + random.nextInt(100));
                            pstmt.setString(15, "Str" + random.nextInt(100));
                            pstmt.executeUpdate();
                        }
                    }
                    Thread.sleep(m);
                }
                JOptionPane.showMessageDialog(null, "Inserção de dados concluída!");
            } catch (SQLException | InterruptedException ex) {
                ex.printStackTrace();
                JOptionPane.showMessageDialog(null, "Erro ao inserir dados: " + ex.getMessage(), "Erro", JOptionPane.ERROR_MESSAGE);
            }
        });
    }

    private static void inserirDadosAleatoriosMySQL(int k, int m, int s) {
        executorService.submit(() -> {
            try (Connection conn = DriverManager.getConnection(mysqlUrl, mysqlUser, mysqlPassword)) {
                Random random = new Random();
                long tempoFinal = System.currentTimeMillis() + s * 1000;

                while (System.currentTimeMillis() < tempoFinal) {
                    for (int i = 0; i < k; i++) {
                        String sql = "INSERT INTO teste (coluna_int1, coluna_int2, coluna_float1, coluna_float2, coluna_float3, coluna_float4, coluna_float5, coluna_data, coluna_str1, coluna_str2, coluna_str3, coluna_str4, coluna_str5, coluna_str6, coluna_str7) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                            pstmt.setInt(1, random.nextInt(100));
                            pstmt.setInt(2, random.nextInt(100));
                            pstmt.setFloat(3, random.nextFloat() * 100);
                            pstmt.setFloat(4, random.nextFloat() * 100);
                            pstmt.setFloat(5, random.nextFloat() * 100);
                            pstmt.setFloat(6, random.nextFloat() * 100);
                            pstmt.setFloat(7, random.nextFloat() * 100);
                            pstmt.setDate(8, new java.sql.Date(System.currentTimeMillis()));
                            pstmt.setString(9, "Str" + random.nextInt(100));
                            pstmt.setString(10, "Str" + random.nextInt(100));
                            pstmt.setString(11, "Str" + random.nextInt(100));
                            pstmt.setString(12, "Str" + random.nextInt(100));
                            pstmt.setString(13, "Str" + random.nextInt(100));
                            pstmt.setString(14, "Str" + random.nextInt(100));
                            pstmt.setString(15, "Str" + random.nextInt(100));
                            pstmt.executeUpdate();
                        }
                    }
                    Thread.sleep(m);
                }
                JOptionPane.showMessageDialog(null, "Inserção de dados concluída!");
            } catch (SQLException | InterruptedException ex) {
                ex.printStackTrace();
                JOptionPane.showMessageDialog(null, "Erro ao inserir dados: " + ex.getMessage(), "Erro", JOptionPane.ERROR_MESSAGE);
            }
        });
    }

    private static void apagarLinhasComProbabilidadeSQLServer(double p) {
        executorService.submit(() -> {
            try (Connection conn = DriverManager.getConnection(sqlServerUrl, sqlServerUser, sqlServerPassword)) {
                Random random = new Random();
                String sql = "SELECT id FROM teste";
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery(sql)) {
                    while (rs.next()) {
                        if (random.nextDouble() < (p / 100)) {
                            String deleteSql = "DELETE FROM teste WHERE id = ?";
                            try (PreparedStatement pstmt = conn.prepareStatement(deleteSql)) {
                                pstmt.setInt(1, rs.getInt("id"));
                                pstmt.executeUpdate();
                            }
                        }
                    }
                }
                JOptionPane.showMessageDialog(null, "Linhas apagadas com " + p + "% de probabilidade!");
            } catch (SQLException ex) {
                ex.printStackTrace();
                JOptionPane.showMessageDialog(null, "Erro ao apagar linhas: " + ex.getMessage(), "Erro", JOptionPane.ERROR_MESSAGE);
            }
        });
    }

    private static void apagarLinhasComProbabilidadeMySQL(double p) {
        executorService.submit(() -> {
            try (Connection conn = DriverManager.getConnection(mysqlUrl, mysqlUser, mysqlPassword)) {
                Random random = new Random();
                String sql = "SELECT id FROM teste";
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery(sql)) {
                    while (rs.next()) {
                        if (random.nextDouble() < (p / 100)) {
                            String deleteSql = "DELETE FROM teste WHERE id = ?";
                            try (PreparedStatement pstmt = conn.prepareStatement(deleteSql)) {
                                pstmt.setInt(1, rs.getInt("id"));
                                pstmt.executeUpdate();
                            }
                        }
                    }
                }
                JOptionPane.showMessageDialog(null, "Linhas apagadas com " + p + "% de probabilidade!");
            } catch (SQLException ex) {
                ex.printStackTrace();
                JOptionPane.showMessageDialog(null, "Erro ao apagar linhas: " + ex.getMessage(), "Erro", JOptionPane.ERROR_MESSAGE);
            }
        });
    }
}