package tinydb.client;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.*;
import javax.swing.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Vector;
import tinydb.util.*;

import javax.swing.filechooser.FileNameExtensionFilter;
import java.io.BufferedReader;
import javax.swing.table.AbstractTableModel;
import javax.swing.table.DefaultTableModel;

import tinydb.server.Main;

public class Swing {
	public static JFrame Loginframe;
	public static JFrame Controlframe;
	public static JPanel ResultPanel;
	public static JLabel displaytime;
	public static InputStream input;
	public static OutputStream output;
	public static int curpos;
	public static long timecost = 0;
	public static long current = 0;
	public static long duration = 0;
	
	
	public static void main(String[] args) throws UnknownHostException, IOException, BadSyntaxException {
		Socket c_socket = new Socket("127.0.0.1", 8888);
		InputStream input_data = c_socket.getInputStream();
		OutputStream output_data = c_socket.getOutputStream();
		input = input_data;
		output = output_data;
		
		JFrame frame1 = new JFrame("TinyDB Login");
		JFrame frame2 = new JFrame("TinyDB");

		frame2.setLayout(null);

		Loginframe = frame1;
		Controlframe = frame2;

		Loginframe.setSize(800, 600);
		Loginframe.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		Loginframe.setResizable(false);
		Controlframe.setSize(800, 600);
		Controlframe.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		Controlframe.setResizable(false);


		JPanel Loginpanel = new JPanel();
		JPanel Resultpanel = new JPanel();
		JPanel Inputpanel = new JPanel();

		ResultPanel = Resultpanel;

		Resultpanel.setBounds(50, 200, 700, 350);
		Inputpanel.setBounds(0, 0, 800, 500);
		Resultpanel.setBorder(BorderFactory.createLoweredBevelBorder());
		Loginframe.add(Loginpanel);
		Controlframe.add(ResultPanel);
		Controlframe.add(Inputpanel);

		placeLoginComponents(Loginpanel);
		placeInputComponents(Inputpanel);

		Loginframe.setVisible(true);
//      Controlframe.setVisible(true);   
	}

	private static void placeLoginComponents(JPanel panel){

		Font font = new Font("ËÎÌו", Font.PLAIN, 15);
		panel.setLayout(null);
		
		JLabel dbLabel = new JLabel("Database:");
		dbLabel.setFont(font);
		dbLabel.setBounds(280,220,80,25);
		panel.add(dbLabel);
		
		JTextField dbText = new JTextField(20);
		dbText.setBounds(360,220,165,25);
		dbText.setText("testdb");
		panel.add(dbText);
		
		JLabel userLabel = new JLabel("User:");

		userLabel.setFont(font);
		userLabel.setBounds(280, 250, 80, 25);
		panel.add(userLabel);


		JTextField userText = new JTextField(20);
		userText.setBounds(360, 250, 165, 25);
		userText.setText("admin");
		panel.add(userText);

		JLabel passwordLabel = new JLabel("Password:");
		passwordLabel.setBounds(280, 280, 80, 25);
		passwordLabel.setFont(font);
		panel.add(passwordLabel);

		JPasswordField passwordText = new JPasswordField(20);
		passwordText.setBounds(360, 280, 165, 25);
		passwordText.setText("admin");
		panel.add(passwordText);

		JButton loginButton = new JButton("login");
		loginButton.setBounds(550, 260, 80, 25);
		panel.add(loginButton);
		byte[] recv = new byte[1024];
		loginButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				String contents = "Login " + dbText.getText() + "\n" + userText.getText() + "\n" + passwordText.getText() + "\n";
				try {
					output.write(contents.getBytes());
					input.read(recv);
					String getto = new String(recv);
					if(getto.charAt(0) == 'O') {
//						System.out.println(new String(recv));
						Loginframe.setVisible(false);
						Controlframe.setVisible(true);
					}
					else {
//						System.out.println("ID or Password is wrong");
					}
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}
		});
	}

	private static void placeInputComponents(JPanel Inputpanel) {

		Main m = new Main();
		Inputpanel.setLayout(null);
		TextArea CMDText = new TextArea(null, 100, 10, TextArea.SCROLLBARS_VERTICAL_ONLY);
		CMDText.setBounds(100, 0, 600, 200);
		JButton Run = new JButton("Run");
		JButton Import = new JButton("Import");
		displaytime = new JLabel("time: ");
		Import.setBounds(10, 30, 80, 25);
		Run.setBounds(720, 30, 60, 25);
		displaytime.setBounds(700,70,100,25);
		JFileChooser fc = new JFileChooser();
		Inputpanel.add(Import);
		Inputpanel.add(Run);
		Inputpanel.add(CMDText);
		Inputpanel.add(displaytime);
		
		Import.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				JFileChooser fileChooser = new JFileChooser(".");
				FileNameExtensionFilter filter = new FileNameExtensionFilter("Text Files", "txt","sql");
				fileChooser.setFileFilter(filter);
				int returnVal = fileChooser.showOpenDialog(fileChooser);
				if (returnVal != JFileChooser.APPROVE_OPTION) {
					return;
				}
				BufferedReader reader = null;
				try {
					reader = new BufferedReader(new FileReader(fileChooser.getSelectedFile()));
				} catch (FileNotFoundException e1) {
					e1.printStackTrace();
				}
				String line;
				StringBuilder sb = new StringBuilder();
				try {
					while ((line = reader.readLine()) != null) {
						sb.append(line);
					}
				} catch (IOException e2) {
					e2.printStackTrace();
				}
				try {
					reader.close();
				} catch (IOException e1) {
					e1.printStackTrace();
				}

				CMDText.setText(sb.toString());
			}
		});

		Run.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				timecost = 0;
				String cmd = CMDText.getText();

				int fst = 0;
				int curpos = 0;
				try {
					for(int i = 0; i < cmd.length(); i++) {
						if(cmd.charAt(i) == ';') {
							String extract = cmd.substring(fst, i);
							fst = i + 1;
							try {
								output.write(extract.getBytes());
								current = System.currentTimeMillis();
								byte[] recvbuffer =  new byte[1024];
								input.read(recvbuffer);
								timecost = timecost + System.currentTimeMillis() - current;
								displaytime.setText("time: " + timecost + "ms");
	//							System.out.println("here0!!!" + cmd);
								ifDisplay((new String(recvbuffer)));
	//							System.out.println((new String(recvbuffer)));
							} catch (IOException e1) {
								e1.printStackTrace();
							}	
						}
					}
				} catch (Exception e3) {
					System.out.println(e3.getMessage());
					JOptionPane.showMessageDialog(null, e3.getMessage().toString(), "Error", JOptionPane.ERROR_MESSAGE);
					return;
				}
				System.out.println("timecost:" + timecost + "ms");
			}
		});
	}
	public static void ifDisplay(String cmd) {
//		String ifSelect = cmd.substring(0, 6);
//		String ifShow = cmd.substring(0,4);
		String fstcmd = Utils.parseFirstCmd(cmd);
//		System.out.println("here1!!!" + cmd);
		if (fstcmd.equals("select")) {
			placeResult(0, cmd.substring(7, cmd.length()));
		}
		else if (fstcmd.contentEquals("show")) {
			placeResult(1, cmd.substring(5, cmd.length()));
		}
		else if (fstcmd.contentEquals("updated")){
			placeResult(2, cmd);
		}
		else {
			placeResult(3, cmd);
		}
			
	}

	private static void placeResult(int type, String cmd) throws BadSyntaxException {
		if(type == 3)
			throw new BadSyntaxException(cmd);
		ResultPanel.removeAll();
		Vector columnNames = new Vector();
		Vector rowData = new Vector();
		Vector Data = new Vector();
		String tableinfo = null;
		String fields = null;
		int fst = 0;
		int cnt = 0;
		if(type == 0) {
			for(int i = 0; i < cmd.length(); i++) {
				if(cmd.charAt(i) == '\n') {
					cnt++;
					if(cnt == 1) {
						tableinfo = cmd.substring(fst, i);
						fst = i + 1;
					}
					else if(cnt == 2) {
						fields = cmd.substring(fst, i);
						fst = i + 1;
						if(!tableinfo.equals("null")) {
							List<String> tmptable = Arrays.asList(tableinfo.substring(1, tableinfo.length() - 1).split(","));
							List<String> tmpfields = Arrays.asList(fields.substring(1, fields.length() - 1).split(","));
							for(int j = 0; j < tmptable.size(); j++) {
								columnNames.add(tmptable.get(j) + "." + tmpfields.get(j));
							}
						}
						else {
							List<String> tmpfields = Arrays.asList(fields.substring(1, fields.length() - 1).split(","));
							for(int j = 0; j < tmpfields.size(); j++) {
								columnNames.add(tmpfields.get(j));
							}
						}
						cmd = cmd.substring(fst, cmd.length() - 1);
						break;
					}
				}
			}
			fst = 0;

			for(int i = 0; i < cmd.length(); i++) {
				if(cmd.charAt(i) == '\t') {
					String test = cmd.substring(fst, i);
					Data.add(cmd.substring(fst, i));
					fst = i + 1;
				}
				else if(cmd.charAt(i) == '\n') {
					rowData.add(Data.clone());
					cnt = 0;
					fst = fst + 1;
					Data.clear();
				}
				else if(cmd.charAt(i) == '\0')
					break;
			}
		}
		else if(type == 1) {
			for(int i = 0; i < cmd.length(); i++) {
				if(cmd.charAt(i) == '\n') {
					columnNames.add(cmd.substring(fst, i));
					fst = i + 1;
				}
				else if(cmd.charAt(i) == '\0')
					break;
			}
		}
		else {
			for (int i = 0; i < cmd.length() ; i++) {
				if(cmd.charAt(i) == '\0') {
					columnNames.add(cmd.substring(0, i));
					break;
				}
			}
		}
				
		JTable table = new JTable(rowData, columnNames);
		DefaultTableModel tableModel = new DefaultTableModel(rowData, columnNames) {

			@Override
			public boolean isCellEditable(int row, int column) {
				// all cells false
				return false;
			}
		};
		table.setModel(tableModel);

		table.setRowHeight(30);

		table.setForeground(Color.BLACK);
		table.setFont(new Font(null, Font.PLAIN, 14)); 
		table.setSelectionForeground(Color.DARK_GRAY); 
		table.setSelectionBackground(Color.LIGHT_GRAY);
		table.setGridColor(Color.GRAY); 

		table.getTableHeader().setFont(new Font(null, Font.BOLD, 14)); 
		table.getTableHeader().setForeground(Color.RED); 
		table.getTableHeader().setResizingAllowed(true);
		table.getTableHeader().setReorderingAllowed(false); 
		
		if(type == 0 || type == 1)
			table.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
		else {
			table.setAutoResizeMode(JTable.AUTO_RESIZE_ALL_COLUMNS);
		}
		
		table.setPreferredScrollableViewportSize(new Dimension(600, 300));
		table.setLocation(-100, 0);

		JScrollPane scrollPane = new JScrollPane(table);
		scrollPane.setViewportView(table);

		
		ResultPanel.add(scrollPane);
		Controlframe.setVisible(true);
	}
}
