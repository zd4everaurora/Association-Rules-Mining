import java.sql.*;
import java.io.*;
import java.math.BigDecimal;
import java.util.*;

/**
 * This file is course project for COP5725 Fall12, applying Association Rules Mining.
 * File name: arm.java
 * Author: Da Zhao
 * Modify Date: Nov 27th, 2012
 **/
class arm {
	public static final int DECIMAL = 5;
	
	// The names of input files 
	public static final String ITMES = "items.dat";
	public static final String TRANS = "trans.dat";
	public static final String INPUT = "system.in";

	// The names of output files 
	public static final String OUTPUT1 = "system.out.1";
	public static final String OUTPUT2 = "system.out.2";
	public static final String OUTPUT3 = "system.out.3";
	public static final String OUTPUT4 = "system.out.4";

	// Delimiters to parse
	public static final String DELIMITER = ",";
	public static final String SUBDELIMITER = "=";

	// The input parameters
	private static String username = "";
	private static String password = "";
	private static String pTask1 = "";
	private static String pTask2 = "";
	private static HashMap<Integer, Comparable> hTask3 = new HashMap<Integer, Comparable>();
	private static HashMap<Integer, Comparable> hTask4 = new HashMap<Integer, Comparable>();

	/********************************** All the pure SQL statements.**********************************/
	// SQLs to drop tables and sequences
	private static final String DROP_FISET = "DROP TABLE FISET";
	private static final String DROP_TRANS = "DROP TABLE TRANS";
	private static final String DROP_ITEMS = "DROP TABLE ITEMS";
	private static final String DROP_FISET_SEQ = "DROP SEQUENCE FISET_SEQ";
	private static final String DROP_TEMP = "DROP TABLE TEMP";
	private static final String DROP_ASSRULE_RESULT = "DROP TABLE ASSRULE_RESULT";
	// SQLs to create tables and sequences
	private static final String CREATE_FISET = "CREATE TABLE FISET (ISET_ID NUMBER(8), ITEM_ID NUMBER(8), PRIMARY KEY (ISET_ID, ITEM_ID), FOREIGN KEY (ITEM_ID) REFERENCES ITEMS(ITEM_ID))";
	private static final String CREATE_FISET_SEQ = "CREATE SEQUENCE FISET_SEQ MINVALUE 1 MAXVALUE 99999999 START WITH 1 INCREMENT BY 1 CYCLE NOCACHE";
	private static final String CREATE_ITEMS = "CREATE TABLE ITEMS (ITEM_ID NUMBER(8) PRIMARY KEY, ITEM_NAME VARCHAR2(30 BYTE))";
	private static final String CREATE_ITEMS_1INDEX = "CREATE INDEX ITEMS_1INDEX ON ITEMS (ITEM_NAME)";
	private static final String CREATE_TRANS = "CREATE TABLE TRANS (TRANS_ID NUMBER(8), ITEM_ID NUMBER(8), PRIMARY KEY (TRANS_ID, ITEM_ID), FOREIGN KEY (ITEM_ID) REFERENCES ITEMS(ITEM_ID))";
	private static final String CREATE_TEMP = "CREATE TABLE TEMP (ITEM_ID NUMBER(8))";
	// SQLs to insert into tables
	private static final String INSERT_ITEMS = "INSERT INTO ITEMS (ITEMS.ITEM_ID, ITEMS.ITEM_NAME) VALUES (?, ?)";
	private static final String INSERT_TRANS = "INSERT INTO TRANS (TRANS.TRANS_ID, TRANS.ITEM_ID) VALUES (?, ?)";
	private static final String INSERT_FISET = "INSERT INTO FISET (FISET.ISET_ID, FISET.ITEM_ID) VALUES (?,?)";
	private static final String INSERT_TEMP = "INSERT INTO TEMP (TEMP.ITEM_ID) VALUES (?)";
	// SQLs to select data from tables
	private static final String SELECT_TOTAL_TRANS = "SELECT COUNT(DISTINCT TRANS_ID) FROM TRANS";
	private static final String SELECT_INITIAL_ASSRULE_RESULT = "SELECT I1.ITEM_ID, I2.ITEM_NAME, I1.NUM/? FROM (SELECT ITEM_ID, COUNT(*) NUM FROM TRANS GROUP BY ITEM_ID) I1, ITEMS I2 WHERE ? <= I1.NUM/? and I1.ITEM_ID = I2.ITEM_ID";
	private static final String FIND_GLUED_FIS = "SELECT DISTINCT IS1.ISET_ID AS ID1, IS2.ISET_ID AS ID2 FROM FISET IS1, FISET IS2 WHERE IS1.ISET_ID > IS2.ISET_ID AND ? = (SELECT COUNT (DISTINCT IS3.ITEM_ID) FROM FISET IS3 WHERE IS1.ISET_ID = IS3.ISET_ID OR IS3.ISET_ID = IS2.ISET_ID)";
	private static final String FIND_CANDIDATE_ITEMSET = "SELECT IT.ITEM_ID, IT.ITEM_NAME FROM ITEMS IT WHERE IT.ITEM_ID IN (SELECT DISTINCT FI.ITEM_ID FROM FISET FI WHERE FI.ISET_ID = ? OR FI.ISET_ID = ?) ORDER BY IT.ITEM_NAME";
	private static final String FIND_SIZE_OF_SUBSET = "SELECT COUNT(*) FROM (SELECT FISet.ISet_ID FROM TEMP, FISet WHERE TEMP.Item_ID = FISet.Item_ID GROUP BY FISet.ISet_ID HAVING COUNT(*) = ?)"; 
	private static final String SELECT_COUNT_FIS = "SELECT COUNT(*) FROM (SELECT COUNT(*) AMOUNT FROM (SELECT TR.TRANS_ID FROM TRANS TR, TEMP TP WHERE TR.ITEM_ID = TP.ITEM_ID) TEM GROUP BY TEM.TRANS_ID) TEP WHERE TEP.AMOUNT = ?";
	// SQLs to delete data from tables
	private static final String DELETE_TEMP = "DELETE FROM TEMP";
	private static final String DELETE_FISET = "DELETE FROM FISET";
	// SQLs to select sequence
	private static final String GETFISET_SEQ = "SELECT FISET_SEQ.NEXTVAL FROM DUAL";
	// Indicate if it requires a new line for the output file
	private static boolean newLineInd = false;
	
	// Inner class, the structure to store the result of Glued Frequent ItemSets
	public class GluedFIs {
		private int FrequentItemSetA;
		private int FrequentItemSetB;
		
		public int getFrequentItemSetA() {
			return FrequentItemSetA;
		}
		public void setFrequentItemSetA(int frequentItemA) {
			FrequentItemSetA = frequentItemA;
		}
		public int getFrequentItemSetB() {
			return FrequentItemSetB;
		}
		public void setFrequentItemSetB(int frequentItemB) {
			FrequentItemSetB = frequentItemB;
		}
	}
	
	// Inner class, the structure to store the result of Candidate Items
	public class CandidateItemSet {
		private int CandidateItemID;
		private String CandidateItemName;
		public int getCandidateItemID() {
			return CandidateItemID;
		}
		public void setCandidateItemID(int candidateItemID) {
			CandidateItemID = candidateItemID;
		}
		public String getCandidateItemName() {
			return CandidateItemName;
		}
		public void setCandidateItemName(String candidateItemName) {
			CandidateItemName = candidateItemName;
		}
	}
	
	// Inner class, the structure to store the result of Initial Association Rule, where the size of output is 1
	public class InitialAssRuleResult {
		private String ItemName;
		private double supportValue;
		public String getItemName() {
			return ItemName;
		}
		public void setItemName(String itemName) {
			ItemName = itemName;
		}
		public double getSupportValue() {
			return supportValue;
		}
		public void setSupportValue(double supportValue) {
			this.supportValue = supportValue;
		}
	}

	
	/**
	 * The main method as an entrance of the project.
	 * @param args
	 * @throws SQLException
	 * @throws IOException
	 */
	public static void main(String args[]) throws SQLException, IOException {

		// Fetch the input information from system.in
		disposeFile(INPUT);
		
		// Drop all tables if already exist
		dropTables();
		// Create the table FISet, TEMP and AssRuleResult
		createTables();
		
		// Fetch the input information from items.dat and trans.dat
		disposeFile(ITMES);
		disposeFile(TRANS);

		// Handle the 4 tasks
		handleTask1();
		handleTask2();
		handleTask3();
		handleTask4();
	}
	
	/**
	 * This method parse the input files:
	 * 			system.in: to generate variables
	 * 			items.dat: to insert items to the table ITEMS
	 * 			trans.dat: to insert trans to the table TRANS
	 * @param fileName		the input file name
	 * @throws SQLException
	 * @throws IOException
	 */
	public static void disposeFile(String fileName) throws SQLException, IOException {
		File file = null;
		BufferedReader reader = null;
		Connection conn = null;
		int[] num = { 0 };
		try {
			file = new File(fileName);
			reader = new BufferedReader(new FileReader(file));
			String tempString = null;
			// Parse the file system.in
			if (INPUT.equals(fileName)) {
				int i = 0;
				String sys_input[] = new String[5];
				while ((tempString = reader.readLine()) != null) {
					sys_input[i] = tempString;
					i++;
				}
				parseInput(sys_input);
			} 
			// Parse the file items.dat
			else if (ITMES.equals(fileName)) {
				conn = getConn(false);
				System.out.println("Start Inserting the data into Items...");
				PreparedStatement ps = conn.prepareStatement(INSERT_ITEMS);
				while ((tempString = reader.readLine()) != null) {
					tempString = tempString.trim();
					if (!"".equals(tempString)) {
						String[] itemTuple = tempString.split(DELIMITER);
						int itemID = Integer.parseInt(itemTuple[0]);
						String itemName = itemTuple[1];
						ps.setInt(1, itemID);
						ps.setString(2, itemName.replace("'", ""));
						ps.addBatch();
					}
				}
				if (ps != null) {
					num = ps.executeBatch();
				}
				System.out.println(num.length + " rows inserted into Items...");
				ps.close();
				conn.commit();
			} 
			// Parse the file trans.dat
			else if (TRANS.equals(fileName)) {
				conn = getConn(false);
				PreparedStatement ps = conn.prepareStatement(INSERT_TRANS);
				System.out.println("Start Inserting the data into Trans...");
				while ((tempString = reader.readLine()) != null) {
					tempString = tempString.trim();
					if (!"".equals(tempString)) {
						String[] itemTuple = tempString.split(DELIMITER);
						int transID = Integer.parseInt(itemTuple[0]);
						int itemID = Integer.parseInt(itemTuple[1]);
						ps.setInt(1, transID);
						ps.setInt(2, itemID);
						ps.addBatch();
					}
				}
				if (ps != null) {
					num = ps.executeBatch();
				}
				System.out.println(num.length + " rows inserted into Trans...");
				ps.close();
				conn.commit();
			}
		} catch (IOException ioe) {
			throw ioe;
		} catch (SQLException sqle) {
			// Rollback the transaction if exception occurs
			if (conn != null) {
				try {
					conn.rollback();
				} catch (SQLException sqe) {
				}
			}
			throw sqle;
		} finally {
			// Close the file reader if possible 
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException ioe) {
				}
			}
			// Close the DB connection if possible
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException sqle) {
				}
			}
		}
	}

	/**
	 * This method is to handle with task1, applying the file system.out.1 as output.
	 * @throws SQLException
	 * @throws IOException
	 * @throws FileNotFoundException
	 */
	public static void handleTask1() throws SQLException, IOException, FileNotFoundException {
		System.out.println("************************************************");
		System.out.println("Start handling Task1...");
		Connection conn = getConn(true);
		FileOutputStream fos = new FileOutputStream(OUTPUT1);
		OutputStreamWriter osw = new OutputStreamWriter(fos);
		BufferedWriter bw = new BufferedWriter(osw);
		taskInitialization(conn);
		final int task1Size = 1;
		handleAprioriRule(task1Size, conn, bw, 1);
		conn.commit();
		bw.close();
		osw.close();
		fos.close();
		conn.close();	
		System.out.println("This is the end of task1");
		System.out.println("************************************************");
	}
	
	/**
	 * This method is to handle with task2, applying the file system.out.2 as output.
	 * @throws SQLException
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static void handleTask2() throws SQLException, FileNotFoundException, IOException {
		System.out.println("************************************************");
		System.out.println("Start handling Task2...");
		final int task2Size = 2;
		Connection conn = getConn(true);
		FileOutputStream fos = new FileOutputStream(OUTPUT2);
		OutputStreamWriter osw = new OutputStreamWriter(fos);
		BufferedWriter bw = new BufferedWriter(osw);
		taskInitialization(conn);
		handleAprioriRule(task2Size, conn, bw, 2);
		conn.commit();
		bw.close();
		osw.close();
		fos.close();
		conn.close();
		System.out.println("This is the end of task2");
		System.out.println("************************************************");
	}

	/**
	 * This method is to handle with task3, applying the file system.out.3 as output.
	 * @throws SQLException
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static void handleTask3() throws SQLException, FileNotFoundException, IOException {
		System.out.println("************************************************");
		System.out.println("Start handling Task3...");
		int task3Size = (Integer) hTask3.get(2);
		Connection conn = getConn(true);
		FileOutputStream fos = new FileOutputStream(OUTPUT3);
		OutputStreamWriter osw = new OutputStreamWriter(fos);
		BufferedWriter bw = new BufferedWriter(osw);
		taskInitialization(conn);
		handleAprioriRule(task3Size, conn, bw, 3);
		conn.commit();
		bw.close();
		osw.close();
		fos.close();
		conn.close();
		System.out.println("This is the end of task3");
		System.out.println("************************************************");
	}

	/**
	 * This method is to handle with task4, applying the file system.out.4 as output.
	 * @throws SQLException
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static void handleTask4() throws SQLException, FileNotFoundException, IOException {
		System.out.println("************************************************");
		System.out.println("Start handling Task4...");
		int task4Size = (Integer) hTask4.get(3);
		Connection conn = getConn(true);
		FileOutputStream fos = new FileOutputStream(OUTPUT4);
		OutputStreamWriter osw = new OutputStreamWriter(fos);
		BufferedWriter bw = new BufferedWriter(osw);
		taskInitialization(conn);
		handleAprioriRule(task4Size, conn, bw, 4);
		conn.commit();
		bw.close();
		osw.close();
		fos.close();
		conn.close();
		System.out.println("This is the end of task4");
		System.out.println("************************************************");
	}
	
	/**
	 * This method is to parse the input file system.in to the member variables.
	 * @param input		parse each line from system.in to set the variables
	 */
	public static void parseInput(String[] input) {
		System.out.println("parsing the input...");
		// parse the line 1: username and password
		String[] temp = input[0].split(DELIMITER);
		username = temp[0].split(SUBDELIMITER)[1].trim();
		password = temp[1].split(SUBDELIMITER)[1].trim();

		// parse the line 2: task1 parameter
		pTask1 = input[1].split(SUBDELIMITER)[1].trim();

		// parse the line 3: task2 parameter
		pTask2 = input[2].split(SUBDELIMITER)[1].trim();

		// parse the line 4: task3 parameters
		temp = input[3].split(DELIMITER);
		hTask3.put(1, new String(temp[0].split(SUBDELIMITER)[1].trim()));
		hTask3.put(2, Integer.valueOf(temp[1].split(SUBDELIMITER)[1].trim()));

		// parse the line 5: task4 parameters
		temp = input[4].split(DELIMITER);
		hTask4.put(1, new String(temp[0].split(SUBDELIMITER)[1].trim()));
		hTask4.put(2, new String(temp[1].split(SUBDELIMITER)[1].trim()));
		hTask4.put(3, Integer.valueOf(temp[2].split(SUBDELIMITER)[1].trim()));
		System.out.println("Input parsed.");
	}
	
	/**
	 * This core method is to handle Apriori Rule for task 1-4, and Association Rule for task 4. Then write the result to output files.
	 * @param size		the output size of the items, this is the maximum size
	 * @param conn		the DB connection, one for a task
	 * @param bw		the buffer writer of the output file
	 * @param tasknum	the task number
	 * @throws SQLException
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static void handleAprioriRule(int size, Connection conn, BufferedWriter bw, int tasknum)	throws SQLException, FileNotFoundException, IOException {
		// The support value of output for task 1-4
		double supportValue = 0.0;
		// The confidence value of output for task 4
		double confidence = 0.0;
		// The String of support value
		String support = "";
		// The output to be written to output files
		String output = "";
		// This HashMap is used to store all the Items Sets that are already been processed, avoiding duplicated process to the table FISET
		HashMap<String, Integer> candidateItemSetPool;
		// This HashMap is used to store the all the result of items from size 1 to maximum, facilitating the query of permutation from task4.
		HashMap<String, Double> resultPool;
		try {
			// Set the support and confidence value
			if (tasknum == 1)
				support = pTask1;
			else if (tasknum == 2)
				support = pTask2;
			else if (tasknum == 3)
				support = (String) hTask3.get(1);
			else if (tasknum == 4){
				support = (String) hTask4.get(1);
				confidence = Double.valueOf(String.valueOf(hTask4.get(2)).replace("%", ""));
			}
			supportValue = Double.valueOf(support.replace("%", "")) / 100;
			// Get the total number of transactions in each task, better avoiding the change of the table TRANS
			double numOfTotalTrans = selectNumOfTotalTrans(conn);
			// Initialize the result pool here to store all the size in the results
			resultPool = new HashMap<String, Double>();
			for (int i = 1; i <= size; i++) {
				int newSize = i;
				// Initialize the candidate pool here to store processed candidate items (may be or not be the result items) for the current size
				candidateItemSetPool = new HashMap<String, Integer>();
				// The table FISET will be empty if new result size is 1
				if(newSize == 1){
					int totalTrans = selectNumOfTotalTrans(conn);
					// Directly invoke selectInitialAssRuleResult to output the result
					InitialAssRuleResult[] initialAssRuleResult = selectInitialAssRuleResult(totalTrans, supportValue, conn, bw, tasknum);
					// Store the result to the result pool
					for (int tmp = 0; tmp < initialAssRuleResult.length; tmp++){
						resultPool.put(initialAssRuleResult[tmp].getItemName(), initialAssRuleResult[tmp].getSupportValue());
					}
					continue;
				}
				// New size is more than 1
				System.out.println("i is: " + i);
				// The list is to store the result and then used to insert into FISET
				List<CandidateItemSet[]> list = new ArrayList<CandidateItemSet[]>(); 
				// step 1: find all possible glued FISets for the new size
				GluedFIs[] gluedFISets = findGluedFISets(newSize, conn);
				if (gluedFISets == null || gluedFISets.length == 0)
					continue;
				// Step 2: for each glued FISets, find all candidate items corresponding to the FISET IDs
				for (int j = 0; j < gluedFISets.length; j++) {
					CandidateItemSet[] candidateItemSet = findCandidateItems(gluedFISets[j], conn);
					if (candidateItemSet == null || candidateItemSet.length == 0) {
						continue;
					}
					// Duplicated check for new Size more than 3. 
					// Take example of {A,B,C}, if {{A,B},{A,C}} are processed, {{A,B},{B,C}} and {{A,C},{B,C}} will not be processed. 
					// Because we will always check if {A,B,C} satisfies the frequent item sets. Otherwise, there maybe 3 {A,B,C} in table FISets.
					if (newSize >= 3){
						String poolKey = "";
						for (int ENUM = 0; ENUM < candidateItemSet.length; ENUM++) {
							poolKey += candidateItemSet[ENUM].getCandidateItemName();
						}
						if (candidateItemSetPool.containsKey(poolKey.trim())) {
							continue;
						}
						else {
							candidateItemSetPool.put(poolKey.trim(), Integer.valueOf(j));
						}
					}
					// Delete the table TEMP
					deleteTemp(conn);
					// Insert the Candidate Item Set in to the table TEMP, which should be the new size
					insertTemp(candidateItemSet, conn);
					// Step 3: find all the subsets whose size is (newSize - 1) that are frequent item sets.
					// This is based on the Apriori Rule
					int subSetSize = findSizeOfSubSet(newSize - 1, conn);
					// This condition means the Item Set whose size is (newSize) MAY BE Frequent Item Set. Otherwise MUST NOT BE.
					if(newSize == subSetSize){
						double numOfFIs = selectNumOfFIs(newSize, conn);
						double supportResult;
						String nameResult;
						if (numOfFIs > 0 && numOfTotalTrans > 0) {
							// This following condition means the Item Set is Frequent Item Set
							if (numOfFIs / numOfTotalTrans >= supportValue) {
								// The support value for this Item Set
								supportResult = new BigDecimal(100 * numOfFIs / numOfTotalTrans).setScale(DECIMAL, BigDecimal.ROUND_HALF_UP).doubleValue();
								// Add it to the list to be inserted into the table FISET
								list.add(candidateItemSet);
								// Construct the output
								output = "{";
								for (int k = 0; k < candidateItemSet.length; k++){
									output = output + candidateItemSet[k].getCandidateItemName() + ",";
								}
								// Store the new Frequent Item Set into result pool
								nameResult = output.substring(1, output.length()-1);
								resultPool.put(nameResult, supportResult);
								// Directly output the support value for task 2 and 3
								if (tasknum < 4) {
									output = output.substring(0, output.length()-1)  + "},s=" + String.valueOf(supportResult) + "%";
						        	if (newLineInd){
						        		bw.newLine();
						        	}
						        	else {
						        		newLineInd = true;
						        	}
									bw.write(output);
								}
								// Calculate the all the possible subsets of this new Frequent Item Set which satisfy the confidence value
								else if (tasknum == 4){
									for (int leftSize = 1; leftSize < newSize; leftSize++){
										// Get all the permutation of the subsets as an array of String
										List<String[]> dividedString = getDividedString(candidateItemSet, 0, leftSize);
										// Traverse all the subsets to calculate if satisfies the confidence value
										Iterator<String[]> it = dividedString.iterator(); 
										while (it.hasNext()){
											String[] temp = it.next();
											double confidenceRes = new BigDecimal(100 * supportResult/resultPool.get(temp[0])).setScale(DECIMAL, BigDecimal.ROUND_HALF_UP).doubleValue();
											if (confidence <= confidenceRes){
												output = "{{" + temp[0] + "} -> {" + temp[1] + "}},s=" + supportResult + "%,c=" + confidenceRes + "%";
									        	if (newLineInd){
									        		bw.newLine();
									        	}
									        	else {
									        		newLineInd = true;
									        	}
												bw.write(output);
											}
										}

									}
								}
							}
						}
					}
				}
				// Insert all the FISets into the table FISET
				if (list.size() != 0) {
					insertFISET(list, conn);
				}
				System.out.println("This is the end of for loop, and i is: " + i);
			}
			conn.commit();
		} catch(SQLException sqle){
			sqle.printStackTrace();
		} catch (FileNotFoundException fnfe) {
			fnfe.printStackTrace();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}
	
	/**
	 * This method is trying to drop all the tables in case the DB env contains them, without throw any exceptions if dropping failed
	 * @throws SQLException
	 */
	public static void dropTables() throws SQLException {
		PreparedStatement ps = null;
		Connection conn = getConn(true);
		try {
			System.out.println("Dropping tables...");
			ps = conn.prepareStatement(DROP_FISET);
			ps.executeUpdate();
		} catch (SQLException sqle) {
		}
		try {
			ps = conn.prepareStatement(DROP_FISET_SEQ);
			ps.executeUpdate();
		} catch (SQLException sqle) {
		}
		try {
			ps = conn.prepareStatement(DROP_TRANS);
			ps.executeUpdate();
		} catch (SQLException sqle) {
		}
		try {
			ps = conn.prepareStatement(DROP_ITEMS);
			ps.executeUpdate();
		} catch (SQLException sqle) {
		}
		try {
			ps = conn.prepareStatement(DROP_ASSRULE_RESULT);
			ps.executeUpdate();
		} catch (SQLException sqle) {
		}
		try {
			ps = conn.prepareStatement(DROP_FISET_SEQ);
			ps.executeUpdate();
		} catch (SQLException sqle) {
		}
		try {
			ps = conn.prepareStatement(DROP_TEMP);
			ps.executeUpdate();
		} catch (SQLException sqle) {
		} finally {
			conn.commit();
			System.out.println("Tables dropped");
			if (ps != null) {
				ps.close();
			}
			if (conn != null) {
				conn.close();
			}
		}
	}

	/**
	 * This method is to drop the tables after trying to drop them
	 * @throws SQLException
	 */
	public static void createTables() throws SQLException {
		System.out.println("Creating tables...");
		PreparedStatement ps = null;
		Connection conn = getConn(true);
		ps = conn.prepareStatement(CREATE_ITEMS);
		ps.executeUpdate();
		ps = conn.prepareStatement(CREATE_ITEMS_1INDEX);
		ps.executeUpdate();
		ps = conn.prepareStatement(CREATE_TRANS);
		ps.executeUpdate();
		ps = conn.prepareStatement(CREATE_FISET);
		ps.executeUpdate();
		ps = conn.prepareStatement(CREATE_FISET_SEQ);
		ps.executeUpdate();
		ps = conn.prepareStatement(CREATE_TEMP);
		ps.executeUpdate();
		conn.commit();
		ps.close();
		conn.close();
		System.out.println("Tables created...");
	}

	/**
	 * This method is to drop the sequence of FISet before each task starts to process
	 * @param Connection	the DB connection
	 * @throws SQLException
	 */
	public static void dropFISetSEQ (Connection conn) throws SQLException {
		PreparedStatement ps = conn.prepareStatement(DROP_FISET_SEQ);
		ps.executeUpdate();
		ps.close();
	}
	
	/**
	 * This method is to create the sequence of FISet before each task starts to process
	 * @param Connection	the DB connection
	 * @throws SQLException
	 */
	public static void createFISetSEQ (Connection conn) throws SQLException {
		PreparedStatement ps = conn.prepareStatement(CREATE_FISET_SEQ);
		ps.executeUpdate();
		ps.close();
	}
	
	/**
	 * This method is to insert the data into the table TEMP
	 * @param CandidateItemSet[]	the data to be inserted
	 * @param Connection			the DB connection
	 * @throws SQLException
	 */
	public static void insertTemp(CandidateItemSet[] candidateItemSet, Connection conn) throws SQLException {
		PreparedStatement ps = conn.prepareStatement(INSERT_TEMP);
		for(int i = 0; i < candidateItemSet.length; i++) {
			ps.setInt(1, candidateItemSet[i].getCandidateItemID());
			ps.addBatch();
		}
		ps.executeBatch();
		ps.close();
	}
	
	/**
	 * This method is to delete the table TEMP before re-use it
	 * @param Connection	the DB connection
	 * @throws SQLException
	 */
	public static void deleteTemp(Connection conn) throws SQLException {
		PreparedStatement ps = conn.prepareStatement(DELETE_TEMP);
		ps.executeUpdate();
		ps.close();
	}
	
	/**
	 * This method is to delete the sequence of FISet before each task starts to process
	 * @param Connection	the DB connection
	 * @throws SQLException
	 */
	public static void deleteFISet(Connection conn) throws SQLException {
		PreparedStatement ps = conn.prepareStatement(DELETE_FISET);
		ps.executeUpdate();
		ps.close();
	}
	
	/**
	 * This method is to generate the output when the new size of Frequent Item Set is 1 (The table FISET is empty)
	 * @param totalTrans		the total amount of transactions
	 * @param supportValue		the minimum support value
	 * @param conn				the DB connection
	 * @param bw				the file buffer writer
	 * @param tasknum			the task number
	 * @return the result of initial Frequent Item Set with size 1 
	 * @throws SQLException
	 * @throws IOException
	 */
	private static InitialAssRuleResult[] selectInitialAssRuleResult (int totalTrans, double supportValue, Connection conn, BufferedWriter bw, int tasknum) throws SQLException, IOException {
		PreparedStatement ps = conn.prepareStatement(SELECT_INITIAL_ASSRULE_RESULT);	
		PreparedStatement ps1 = conn.prepareStatement(INSERT_FISET);
		ps.setInt(1, totalTrans);
		ps.setDouble(2, supportValue);
		ps.setInt(3, totalTrans);
		ResultSet rs = ps.executeQuery();
		// Store each new Frequent Item Set
		Vector v = new Vector();
		while (rs.next()) {
			double supportRes = new BigDecimal(rs.getDouble(3) * 100).setScale(DECIMAL, BigDecimal.ROUND_HALF_UP).doubleValue();
			InitialAssRuleResult tuple = createArm().new InitialAssRuleResult();
			tuple.setItemName(rs.getString(2));
			tuple.setSupportValue(supportRes);
			v.addElement(tuple);
	        // output the size-1 result for the task 1-3. Task 4 requires confidence value and its size is from 2.
	        if (tasknum < 4) {
	        	if (newLineInd){
	        		bw.newLine();
	        	}
	        	else {
	        		newLineInd = true;
	        	}
				bw.write("{" + rs.getString(2) + "},s=" + String.valueOf(supportRes) + "%");
	        }
			int seqNum = getFISetSeqNum(conn);						
			ps1.setInt(1, seqNum);
			ps1.setInt(2, rs.getInt(1));
			ps1.addBatch();
		}
		ps1.executeBatch();
		ps1.close();
		ps.close();
		rs.close();
		if (v.size() == 0){
			return null;
		}
		InitialAssRuleResult[] list = new InitialAssRuleResult[v.size()];
		v.toArray(list);
		return list;
	}
	
	/**
	 * This method is to find all the glued Frequent Item Sets which contain new size of Items
	 * @param newSize	the new size
	 * @param conn		the DB connection
	 * @return	the objection storing the pair of FI Sets
	 * @throws SQLException
	 */
	private static GluedFIs[] findGluedFISets(int newSize, Connection conn) throws SQLException {
		PreparedStatement ps = conn.prepareStatement(FIND_GLUED_FIS);	
		ps.setInt(1, newSize);
		ResultSet rs = ps.executeQuery();
		Vector v = new Vector();
		while (rs.next()) {
	        GluedFIs tuple = createArm().new GluedFIs();
	        tuple.setFrequentItemSetA(rs.getInt(1));
	        tuple.setFrequentItemSetB(rs.getInt(2));
	        v.addElement(tuple);
		}
		ps.close();
		rs.close();
		if (v.size() == 0){
			return null;
		}
		GluedFIs[] list = new GluedFIs[v.size()];
		v.toArray(list);
		return list;
	}
	
	/**
	 * This method is to find all the candidate items from glued FI
	 * @param gluedFI	the pair of FI Sets
	 * @param conn		the DB connection
	 * @return: all the candidate Item Sets
	 * @throws SQLException
	 */
	private static CandidateItemSet[] findCandidateItems(GluedFIs gluedFI, Connection conn) throws SQLException {
		PreparedStatement ps = conn.prepareStatement(FIND_CANDIDATE_ITEMSET);
		ps.setInt(1, gluedFI.getFrequentItemSetA());
		ps.setInt(2, gluedFI.getFrequentItemSetB());
		ResultSet rs = ps.executeQuery();
		Vector v = new Vector();
		while (rs.next()) {
			CandidateItemSet tuple = createArm().new CandidateItemSet();
	        tuple.setCandidateItemID(rs.getInt(1));
	        tuple.setCandidateItemName(rs.getString(2));
	        v.addElement(tuple);
		}
		ps.close();
		rs.close();
		if (v.size() == 0){
			return null;
		}
		CandidateItemSet[] list = new CandidateItemSet[v.size()];
		v.toArray(list);
		return list;
	}
	
	/**
	 * This method is to find the size of all subsets, based on the table TEMP.
	 * @param currSize	the current size (new size - 1)
	 * @param conn		the DB connection
	 * @return  		the amount of subsets
	 * @throws SQLException
	 */
	public static int findSizeOfSubSet(int currSize, Connection conn) throws SQLException{
		PreparedStatement ps = conn.prepareStatement(FIND_SIZE_OF_SUBSET);
		ps.setInt(1, currSize);
		ResultSet rs = ps.executeQuery();
		if (rs.next()) {
			int result = rs.getInt(1);
			ps.close();
			rs.close();
			return result;
		}
		return 0;
	}
	
	/**
	 * This method is to get the amount of Frequent Items in the table TRANS, based on the table TEMP
	 * @param newSize	the new size
	 * @param conn  	the DB connection
	 * @return
	 * @throws SQLException
	 */
	public static int selectNumOfFIs (int newSize, Connection conn) throws SQLException{
		PreparedStatement ps = conn.prepareStatement(SELECT_COUNT_FIS);
		ps.setInt(1, newSize);
		ResultSet rs = ps.executeQuery();
		if (rs.next()) {
			int result = rs.getInt(1);
			ps.close();
			rs.close();
			return result;
		}
		return 0;
	}
	
	/**
	 * This method is to get the amount of total transactions from the table TRANS
	 * @param conn		the DB connection
	 * @return			the amount of total transactions
	 * @throws SQLException
	 */
	public static int selectNumOfTotalTrans (Connection conn) throws SQLException{
		PreparedStatement ps = conn.prepareStatement(SELECT_TOTAL_TRANS);
		ResultSet rs = ps.executeQuery();
		if (rs.next()) {
			int result = rs.getInt(1);
			ps.close();
			rs.close();
			return result;
		}
		return 0;
	}
	
	/**
	 * This method is to insert the new Frequent Items into table FISET
	 * @param list		the data to be inserted
	 * @param conn		the DB connection
	 * @return
	 * @throws SQLException
	 */
	public static int insertFISET(List<CandidateItemSet[]> list, Connection conn) throws SQLException{
		PreparedStatement ps = conn.prepareStatement(INSERT_FISET);
		Iterator<CandidateItemSet[]> it = list.iterator(); 
		while (it.hasNext()){
			CandidateItemSet[] candidateItemSet= (CandidateItemSet[]) it.next();
			int seqNum = getFISetSeqNum(conn);
			for (int i = 0; i < candidateItemSet.length; i++) {
				ps.setInt(1, seqNum);
				ps.setInt(2, candidateItemSet[i].getCandidateItemID());
				ps.addBatch();
			}
		}
		int num[] = ps.executeBatch();
		return num.length;
	}

	/**
	 * This method is to get the connection from DB
	 * @param autoCommit		Y: autoCommit	N: not automatically commit, used for batch update
	 * @return	the DB connection
	 * @throws SQLException
	 */
	private static Connection getConn(boolean autoCommit) throws SQLException {
		DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
		Connection conn = DriverManager.getConnection(
				"jdbc:oracle:thin:hr/hr@oracle1.cise.ufl.edu:1521:orcl",
				username, password);
		conn.setAutoCommit(autoCommit);
		return conn;
	}

	/**
	 * This method is to get the sequence number of the table FISET
	 * @param conn			the DB connection
	 * @return	the next value of sequence
	 * @throws SQLException
	 */
	private static int getFISetSeqNum(Connection conn) throws SQLException {
		int res;
		PreparedStatement ps = conn.prepareStatement(GETFISET_SEQ);
		ResultSet rs = ps.executeQuery();
		if (!rs.next())
			throw new SQLException("Failed getting sequence number using query:");
		res = rs.getInt(1);
		ps.close();
		rs.close();
		System.out.println("Got the sequence number of FISET is: " + res);
		return res;
	}
	
	/**
	 * This method is to initialize the table for each task
	 * @param conn			the DB connection
	 * @throws SQLException
	 */
	private static void taskInitialization(Connection conn) throws SQLException {
		newLineInd = false;
		try {
			deleteTemp(conn);
		} catch (SQLException sqle){
		}
		try {
			deleteFISet(conn);
		} catch (SQLException sqle){
		}
		try {
			dropFISetSEQ(conn);
		} catch (SQLException sqle){
		}
		try {
			createFISetSEQ(conn);
		} catch (SQLException sqle){
			sqle.printStackTrace();
			throw sqle;
		}
	}
	
	/**
	 * This method is to calculate all the possible permutations for the Candidate Item Set, using divide and conquer.
	 * For size i of the String, we will get the FIRST one as leftString, and calculate the rest String with starting point plus 1 and size subtract 1.
	 * Then change the FIRST one to the next, and so on...
	 * For example: {A,B,C} should return {{A},{B,C}}, {{B},{A,C}}, {{C},{A,B}}, {{A,B},{C}}, {{A,C},{B}} and {{B,C},{A}}
	 * @param candidateItemSet		the Candidate Item Set with the Item Names
	 * @param start					the start point of the array, to be used in divide and conquer
	 * @param size					the exact left size of the Candidate Item Set
	 * @return
	 */
	private static List<String[]> getDividedString (CandidateItemSet[] candidateItemSet, int start, int size) {
		List<String[]> resString = new ArrayList<String[]>();
		List<String[]> tmpString = new ArrayList<String[]>(); 
		for (int i = start; i < candidateItemSet.length; i++) {
			String leftRes = "";
			String rightRes = "";
			if (size == 1){
				leftRes = candidateItemSet[i].getCandidateItemName();
				for (int j = start; j < candidateItemSet.length; j++) {
					if (j != i) {
						if ("".equals(rightRes.trim())){
							rightRes = candidateItemSet[j].getCandidateItemName();
						}
						else {
							rightRes = rightRes + "," + candidateItemSet[j].getCandidateItemName();
						}
					}
				}
				resString.add(new String[]{leftRes, rightRes});
				continue;
			}
			if (i > start) {
				for (int j = start; j < i; j++){
					rightRes = ("".equals(rightRes.trim())) ? candidateItemSet[j].getCandidateItemName() : rightRes + "," + candidateItemSet[j].getCandidateItemName();
				}
			}
			leftRes = candidateItemSet[i].getCandidateItemName();
			tmpString = getDividedString (candidateItemSet, i + 1, size - 1);
			Iterator<String[]> it = tmpString.iterator(); 
			while (it.hasNext()){
				String[] temp = it.next();
				String addRightRes = ("".equals(temp[1].trim())) ? rightRes:(("".equals(rightRes.trim())) ? temp[1] : rightRes + "," + temp[1]);
				resString.add(new String[]{leftRes + "," + temp[0], addRightRes});
			}
		}
		return resString;
	}
	
	/**
	 * This method is to create the class object itself, to apply for the inner class
	 * @return		the class object itself
	 */
	public static arm createArm() {
		arm a = new arm();
		return a;	
	}
}