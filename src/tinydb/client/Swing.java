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
	
	public static void main(String[] args) throws UnknownHostException, IOException {
		// 创建 JFrame 实例
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
		// Setting the width and height of frame
		Loginframe.setSize(800, 600);
		Loginframe.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		Loginframe.setResizable(false);
		Controlframe.setSize(800, 600);
		Controlframe.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		Controlframe.setResizable(false);

		/*
		 * 创建面板，这个类似于 HTML 的 div 标签 我们可以创建多个面板并在 JFrame 中指定位置 面板中我们可以添加文本字段，按钮及其他组件。
		 */
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
		/*
		 * 调用用户定义的方法并添加组件到面板
		 */
		placeLoginComponents(Loginpanel);
		placeInputComponents(Inputpanel);
		// 设置界面可见
		Loginframe.setVisible(true);
//      Controlframe.setVisible(true);   
	}

	private static void placeLoginComponents(JPanel panel){

		/*
		 * 布局部分我们这边不多做介绍 这边设置布局为 null
		 */
		Font font = new Font("宋体", Font.PLAIN, 15);
		panel.setLayout(null);

		// 创建 JLabel
		JLabel userLabel = new JLabel("User:");
		/*
		 * 这个方法定义了组件的位置。 setBounds(x, y, width, height) x 和 y 指定左上角的新位置，由 width 和 height
		 * 指定新的大小。
		 */
		userLabel.setFont(font);
		userLabel.setBounds(280, 250, 80, 25);
		panel.add(userLabel);

		/*
		 * 创建文本域用于用户输入
		 */
		JTextField userText = new JTextField(20);
		userText.setBounds(360, 250, 165, 25);
		panel.add(userText);

		JLabel passwordLabel = new JLabel("Password:");
		passwordLabel.setBounds(280, 280, 80, 25);
		passwordLabel.setFont(font);
		panel.add(passwordLabel);

		JPasswordField passwordText = new JPasswordField(20);
		passwordText.setBounds(360, 280, 165, 25);
		panel.add(passwordText);

		// 创建登录按钮
		JButton loginButton = new JButton("login");
		loginButton.setBounds(550, 260, 80, 25);
		panel.add(loginButton);
		byte[] recv = new byte[1024];
		loginButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				String contents = "Login " + userText.getText() + " " + passwordText.getText();
				try {
					output.write(contents.getBytes());
					input.read(recv);
					String getto = new String(recv);
					System.out.println("get:" + getto);
					if(getto.charAt(0) == 'N') {
						System.out.println(new String(recv));
						Loginframe.setVisible(false);
						Controlframe.setVisible(true);
					}
					else {
						System.out.println("ID or Password is wrong");
					}
				} catch (IOException e1) {
					// TODO Auto-generated catch block
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
//      fc.setDialogTitle("import");
//      fc.setBounds(720, 50, 800, 600);
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
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				String line;
				StringBuilder sb = new StringBuilder();
				try {
					while ((line = reader.readLine()) != null) {
						sb.append(line);
					}
				} catch (IOException e2) {
					// TODO Auto-generated catch block
					e2.printStackTrace();
				}
				try {
					reader.close();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
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
				for(int i = 0; i < cmd.length(); i++) {
					if(cmd.charAt(i) == ';') {
						String extract = cmd.substring(fst, i);
						fst = i + 1;
						try {
							System.out.println(extract);
							output.write(extract.getBytes());
							System.out.println(extract);
							input.read(recvbuffer);
							System.out.println(extract + (new String(recvbuffer)));
						} catch (IOException e1) {
							e1.printStackTrace();
						}	
					}
				}
			}
		});
	}

	private static void placeResultComponents() {

		String[] columnNames = { "序号", "姓名", "语文", "数学", "英语", "总分", "生日" };
		// 表格所有行数据
		Object[][] rowData = { { 1, "张三", 80, 80, 80, 240, " " }, { 2, "John", 70, 80, 90, 240, " " },
				{ 3, "Sue", 70, 70, 70, 210, " " }, { 4, "Jane", 80, 70, 60, 210, " " },
				{ 5, "Joe_05", 80, 70, 60, 210, " " }, { 6, "Joe_06", 80, 70, 60, 210, " " },
				{ 7, "Joe_07", 80, 70, 60, 210, " " }, { 8, "Joe_08", 80, 70, 60, 210, " " },
				{ 9, "Joe_09", 80, 70, 60, 210, " " }, { 10, "Joe_10", 80, 70, 60, 210, " " },
				{ 11, "Joe_11", 80, 70, 60, 210, " " }, { 12, "Joe_12", 80, 70, 60, 210, " " },
				{ 13, "Joe_13", 80, 70, 60, 210, " " }, { 14, "Joe_14", 80, 70, 60, 210, " " },
				{ 15, "Joe_15", 80, 70, 60, 210, " " }, { 16, "Joe_16", 80, 70, 60, 210, " " },
				{ 17, "Joe_17", 80, 70, 60, 210, " " }, { 18, "Joe_18", 80, 70, 60, 210, " " },
				{ 19, "Joe_19", 80, 70, 60, 210, " " }, { 20, "Joe_20", 80, 70, 60, 210, " " } };

		// 创建一个表格，指定 表头 和 所有行数据

		JTable table = new JTable(rowData, columnNames);
		DefaultTableModel tableModel = new DefaultTableModel(rowData, columnNames) {

			@Override
			public boolean isCellEditable(int row, int column) {
				// all cells false
				return false;
			}
		};
		table.setModel(tableModel);
		// 设置行高
		table.setRowHeight(30);

		// 设置表格内容颜色
		table.setForeground(Color.BLACK); // 字体颜色
		table.setFont(new Font(null, Font.PLAIN, 14)); // 字体样式
		table.setSelectionForeground(Color.DARK_GRAY); // 选中后字体颜色
		table.setSelectionBackground(Color.LIGHT_GRAY); // 选中后字体背景
		table.setGridColor(Color.GRAY); // 网格颜色

		// 设置表头
		table.getTableHeader().setFont(new Font(null, Font.BOLD, 14)); // 设置表头名称字体样式
		table.getTableHeader().setForeground(Color.RED); // 设置表头名称字体颜色
		table.getTableHeader().setResizingAllowed(false); // 设置不允许手动改变列宽
		table.getTableHeader().setReorderingAllowed(false); // 设置不允许拖动重新排序各列
		table.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);

		// 设置滚动面板视口大小（超过该大小的行数据，需要拖动滚动条才能看到）
		table.setPreferredScrollableViewportSize(new Dimension(600, 300));
		table.setLocation(-100, 0);

		JScrollPane scrollPane = new JScrollPane(table);
		scrollPane.setViewportView(table);

		ResultPanel.add(scrollPane);
		Controlframe.setVisible(true);
	}
}
