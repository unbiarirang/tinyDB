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
import java.util.Vector;

import javax.swing.filechooser.FileNameExtensionFilter;
import java.io.BufferedReader;
import javax.swing.table.AbstractTableModel;
import javax.swing.table.DefaultTableModel;

import tinydb.server.Main;

public class Swing {
	public static JFrame Loginframe;
	public static JFrame Controlframe;
	public static JPanel ResultPanel;
	public static InputStream input;
	public static OutputStream output;
	public static int curpos;
	
	public static void main(String[] args) throws UnknownHostException, IOException {
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
		Controlframe.add(Resultpanel);
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
					System.out.println("get:" + getto);
					if(getto.charAt(0) == 'O') {
						System.out.println(new String(recv));
						Loginframe.setVisible(false);
						Controlframe.setVisible(true);
					}
					else {
						System.out.println("ID or Password is wrong");
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
		Import.setBounds(10, 30, 80, 25);
		Run.setBounds(720, 30, 60, 25);
		JFileChooser fc = new JFileChooser();
		Inputpanel.add(Import);
		Inputpanel.add(Run);
		Inputpanel.add(CMDText);
		Import.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				JFileChooser fileChooser = new JFileChooser(".");
				FileNameExtensionFilter filter = new FileNameExtensionFilter("Text Files", "txt");
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
				String cmd = CMDText.getText();
				byte[] recvbuffer =  new byte[1024];
				int fst = 0;
				int curpos = 0;
				for(int i = 0; i < cmd.length(); i++) {
					if(cmd.charAt(i) == ';') {
						String extract = cmd.substring(fst, i);
						fst = i + 1;
						try {
							output.write(extract.getBytes());
							input.read(recvbuffer);
							ifSelect((new String(recvbuffer)));
							System.out.println((new String(recvbuffer)));
						} catch (IOException e1) {
							e1.printStackTrace();
						}	
					}
				}
			}
		});
	}
	public static void ifSelect(String cmd) {
		String test = cmd.substring(0, 6);
		if (test.equals("select")) {
			placeResultComponents(cmd.substring(7, cmd.length()));
		}
	}

	private static void placeResultComponents(String cmd) {
		
		Vector columnNames = new Vector();
		int columnNum = 0;
		for(int i = 0; i < cmd.length(); i++) {
			if(cmd.charAt(i) == ' ') {
				columnNum = Integer.parseInt(cmd.substring(0, i));
				cmd = cmd.substring(i + 1, cmd.length());
				break;
			}
		}
		int fst = 0;
		int cnt = 0;
		for(int i = 0; i < cmd.length(); i++) {
			if (cmd.charAt(i) == '\n') {
				cnt++;
				String test = cmd.substring(fst, i);
				columnNames.add(cmd.substring(fst, i));
				fst = i;
				if(cnt == columnNum) {
					cmd = cmd.substring(fst + 1, cmd.length());
					break;
				}
			}
		}
		Vector rowData = new Vector();
	
		fst = 0;
		cnt = 0;
		Vector Data = new Vector();
		for(int i = 0; i < cmd.length(); i++) {
			if(cmd.charAt(i) == '\n') {
				String test = cmd.substring(fst, i);
				Data.add(cmd.substring(fst, i));
				fst = i + 1;
				cnt++;
				if(cnt == columnNum){
					rowData.add(Data.clone());
					cnt = 0;
					Data.clear();
				}
			}
		}
		
		
		ResultPanel.removeAll();
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
		table.getTableHeader().setResizingAllowed(false); 
		table.getTableHeader().setReorderingAllowed(false); 
		table.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);

		
		table.setPreferredScrollableViewportSize(new Dimension(600, 300));
		table.setLocation(-100, 0);

		JScrollPane scrollPane = new JScrollPane(table);
		scrollPane.setViewportView(table);

		ResultPanel.add(scrollPane);
		Controlframe.setVisible(true);
	}
}
