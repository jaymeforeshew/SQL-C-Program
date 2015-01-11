import java.sql.*;
import java.util.Properties;
import java.util.*;
import java.io.*;


public class A3 {

	// Class Variables
	private Connection connection;
	private String connection_string;
	private String input_file;

	// Helper function- checks if Matrix Exists in database	
	public boolean matrixExists( int index, int row_dim, int col_dim ) throws SQLException 
	{
		String query = "SELECT * FROM MATRIX WHERE MATRIX_ID = " + index + " AND ROW_DIM >= " + row_dim + " AND COL_DIM >= " + col_dim + ";";
		Statement statement = connection.createStatement();
		ResultSet resultSet = statement.executeQuery( query );
		int numRows = 0;
		if( resultSet.last() ){
			numRows = resultSet.getRow();    
		}

		if( numRows == 0 ) return false;
		return true;
	}
	
	// Get the value of a particular Matrix Cell
	public double getValue( int index, int row_num, int col_num, boolean printMessage ) throws SQLException 
	{
		// Variables
		String query;
		Statement statement;
		ResultSet resultSet;
		
		// Check if Matrix exists
		if( matrixExists( index, row_num, col_num ) == false ) {
		    	if( printMessage == true ) System.out.println( "ERROR" );
		    	return 0;
		}
			
		// Matrix exists, look for value
		query = "SELECT * FROM MATRIX_DATA WHERE MATRIX_ID = " + index + " AND ROW_NUM = " + row_num + " AND COL_NUM = " + col_num + ";";
		statement = connection.createStatement();
		resultSet = statement.executeQuery( query );
		
		if( resultSet.first() ) {
			double value = resultSet.getDouble( "VALUE" );
			if( printMessage == true ) System.out.println( value );
			return value;
		} else {
		    	if( printMessage == true ) System.out.println( "ERROR" );
			return 0;
		}
	}

	// Set Value of a particular Matrix Cell
	public void setValue( int index, int row_num, int col_num, double value, boolean printMessage  ) throws SQLException 
	{
		// Don't insert entries with value 0
		if( value == 0 ) {
			if( printMessage == true ) System.out.println( "DONE" );
			return;
		}

		// Variables
		String query;
		Statement statement;
		ResultSet resultSet;
		
		// Check if matrix exists
		if ( matrixExists( index, row_num, col_num ) == false ) {
			if( printMessage == true ) System.out.println( "ERROR" );    
			return;
		}
		
		// Matrix exists, delete previous entry in this cell if necessary
		query = "DELETE FROM MATRIX_DATA WHERE MATRIX_ID = " + index + " AND ROW_NUM = " + row_num + " AND COL_NUM = " + col_num + ";";
		statement = connection.createStatement();
		statement.executeUpdate( query );

		// Insert Value into MATRIX_DATA
		query = "INSERT INTO MATRIX_DATA VALUES (" + index + ", " + row_num + ", " + col_num + ", " + value + ");";
		statement = connection.createStatement();
		statement.executeUpdate( query );
		
		if( printMessage == true ) System.out.println( "DONE" );
		return;
	}

	// Insert Matrix into Database, or expand/contract if possible
	public void setMatrix( int index, int row_dim, int col_dim, boolean printMessage ) throws SQLException 
	{
		// Error Checking
		if( row_dim < 1 || col_dim < 1 ) {
		    if( printMessage == true ) System.out.println( "ERROR" );
		    return;
		}
		
		// Variables
		String query;
		Statement statement;
		ResultSet resultSet;
		
		// Check if Matrix already in db
		query = "SELECT * FROM MATRIX WHERE MATRIX_ID = " + index + ";";
		statement = connection.createStatement();
		resultSet = statement.executeQuery( query );
		int numRows = 0;
		if( resultSet.last() ) {
		    numRows = resultSet.getRow();
		} 
		
		if( numRows == 0 ) {
			// does not exist, create Matrix
			query = "INSERT INTO MATRIX VALUES " + "( " + index + ", " + row_dim + ", " + col_dim + ");";
			statement = connection.createStatement();
			statement.executeUpdate( query );    
		} else {
		    	// change dimensions of matrix if possible
			int curr_rowDim = resultSet.getInt( "ROW_DIM" );
			int curr_colDim = resultSet.getInt( "COL_DIM" );
			
			// if matrix dimensions are the same, then do nothing
			if( row_dim == curr_rowDim && col_dim == curr_colDim ) {
				if( printMessage == true ) System.out.println( "DONE" ); 
				return;   
			}
			
			if( row_dim < curr_rowDim ) {
				query = "SELECT * FROM MATRIX_DATA WHERE MATRIX_ID = " + index + " AND ROW_NUM > " + row_dim + ";";
				statement = connection.createStatement();
				resultSet = statement.executeQuery( query );
				
				numRows = 0;
				if( resultSet.last() ) {
					numRows = resultSet.getRow(); 	   
				}

				if( numRows > 0 ) { // Cannot contract matrix
					if( printMessage == true ) System.out.println( "ERROR" );
					return;			 	   
				}
			}

			if( col_dim < curr_colDim ) {
				query = "SELECT * FROM MATRIX_DATA WHERE MATRIX_ID = " + index + " AND COL_NUM > " + col_dim + ";";
				statement = connection.createStatement();
				resultSet = statement.executeQuery( query );
				
				numRows = 0;
				if( resultSet.last() ) {
					numRows = resultSet.getRow();
				}
				
				if( numRows > 0 ) { // Cannot contract matrix
					if( printMessage == true ) System.out.println( "ERROR" );
					return;    	
				}

			}

			// Matrix is safe to contract or expand
			query = "UPDATE MATRIX SET ROW_DIM = " + row_dim + ", COL_DIM = " + col_dim + " WHERE MATRIX_ID = " + index + ";";  
			statement = connection.createStatement();
			statement.executeUpdate( query );	
		}
		
		if( printMessage == true ) System.out.println( "DONE" );	
			
	}

	// Delete Matrix index and its data
	public void deleteMatrix( int index, boolean shouldPrint ) throws SQLException
	{
		// Variables
		String query;
		Statement statement;

		// Delete Matrix 
		query = "DELETE FROM MATRIX WHERE MATRIX_ID = " + index + ";";
		statement = connection.createStatement();
		statement.executeUpdate( query );

		// Delete Matrix Data
		query = "DELETE FROM MATRIX_DATA WHERE MATRIX_ID = " + index + ";";
		statement = connection.createStatement();
		statement.executeUpdate( query );
		
		if( shouldPrint == true ) {
		    	System.out.println( "DONE" );
		}
		return;	
	}

	// Delete all Matrices and Matrix Data
	public void deleteAll() throws SQLException
	{
		// Variables
		String query;
		Statement statement;

		// Delete all Matrices
		query = "DELETE FROM MATRIX;";
		statement = connection.createStatement();
		statement.executeUpdate( query );

		// Delete all Matrix Data
		query = "DELETE FROM MATRIX_DATA;";
		statement = connection.createStatement();
		statement.executeUpdate( query );

		System.out.println( "DONE" );
		return;
		
	}
	
	// Returns the dimension of a Matrix 
	// If row == true, return row, otherwise return column dimension 
	public int getDimension( int index, boolean row ) throws SQLException
	{
		// Variables
		String query;
		Statement statement;
		ResultSet resultSet;
		
		// Get Matrix Information
		query = "SELECT * FROM MATRIX WHERE MATRIX_ID = " + index + ";";
		statement = connection.createStatement();
		resultSet = statement.executeQuery( query );
		
		if( resultSet.first() ) {
		    	if( row == true ) {
				return resultSet.getInt( "ROW_DIM" );
			} else {
			    	return resultSet.getInt( "COL_DIM" );
			}
		} else {
		    	return -1;
		}
	}

	// Returns true if the two Matrices have the same dimensions (ie. are addition-compatable)
	public boolean sameDimensions( int index1, int index2 ) throws SQLException
	{
		// Variables
		String query;
		Statement statement;
		ResultSet resultSet;
		int index1_rows;
		int index1_cols;
		int index2_rows;
		int index2_cols;

		// Get Matrix 1 Dimensions	
		query = "SELECT * FROM MATRIX WHERE MATRIX_ID = " + index1 + ";";
		statement = connection.createStatement();
		resultSet = statement.executeQuery( query );
		if( resultSet.first() ) {
			index1_rows = resultSet.getInt( "ROW_DIM" );
			index1_cols = resultSet.getInt( "COL_DIM" );
		} else {
		    	return false;
		}

		// Get Matrix 2 Dimensions
		query = "SELECT * FROM MATRIX WHERE MATRIX_ID = " + index2 + ";";
		statement = connection.createStatement();
		resultSet = statement.executeQuery( query );
		if( resultSet.first() ) {
		    	index2_rows = resultSet.getInt( "ROW_DIM" );
			index2_cols = resultSet.getInt( "COL_DIM" );
		} else {
			return false;
		}

		// Compare Dimensions
		if( index1_rows == index2_rows && index1_cols == index2_cols ) {
		    	return true;
		} else {
		    	return false;
		}
	}

	// Add or subtract Matrix 1 +/- Matrix 2, placing result in resultMatrix; if toAdd == true, Add. If false, subtract
	public void addSubtractMatrix( int resultMatrix, int matrix1, int matrix2, boolean toAdd ) throws SQLException
	{
		// Variables
		String query;
		Statement statement;
		ResultSet matrix1_resultSet;
		ResultSet matrix2_resultSet;
		
		// Check addition compatability
		if( sameDimensions( matrix1, matrix2 ) == false ) {
			System.out.println( "ERROR" );
			return;
		}

		// Delete any existing entries for the result Matrix
		deleteMatrix( resultMatrix, false );
		
		// Get Matrix Dimensions
		int rows = getDimension( matrix1, true );
		int cols = getDimension( matrix1, false );
		
		if( rows == -1 || cols == -1 ) {
			System.out.println( "ERROR" );
			return;
		}

		// Create resultMatrix
		query = "INSERT INTO MATRIX VALUES (" + resultMatrix + ", " + rows + ", " + cols + ");";
		statement = connection.createStatement();
		statement.executeUpdate( query );

		// Place Matrix1's values into resultMatrix
		query = "SELECT * FROM MATRIX_DATA WHERE MATRIX_ID = " + matrix1 + ";";
		statement = connection.createStatement();
		matrix1_resultSet = statement.executeQuery( query );		
		
		while( matrix1_resultSet.next() ) {
			int rowDim = matrix1_resultSet.getInt( "ROW_NUM" );
			int colDim = matrix1_resultSet.getInt( "COL_NUM" );	
			double value = matrix1_resultSet.getDouble( "VALUE" );
			query = "INSERT INTO MATRIX_DATA VALUES (" + resultMatrix + ", " + rowDim + ", " + colDim + ", " + value + ");";
			statement = connection.createStatement();
			statement.executeUpdate( query );
		}

		// Place Matrix2's values into resultMatrix; adding where necessary
		query = "SELECT * FROM MATRIX_DATA WHERE MATRIX_ID = " + matrix2 + ";";
		statement = connection.createStatement();
		matrix2_resultSet = statement.executeQuery( query );
		
		while( matrix2_resultSet.next() ) {
		    	int rowDim = matrix2_resultSet.getInt( "ROW_NUM" );
			int colDim = matrix2_resultSet.getInt( "COL_NUM" );
			double value = matrix2_resultSet.getDouble( "VALUE" );				    	 		
			if( toAdd == false ) {
				value = value * -1;
			}	
			// Check if there is a value in the same cell in resultMatrix
			double currValue = getValue( resultMatrix, rowDim, colDim, false );
			if( currValue == 0 ) {	// No addition necessary
				query = "INSERT INTO MATRIX_DATA VALUES (" + resultMatrix + ", " + rowDim + ", " + colDim + ", " + value + ");";
				statement = connection.createStatement();
				statement.executeUpdate( query );		    	            
			} else {  // add values
				double addedValue = currValue + value;
				setValue( resultMatrix, rowDim, colDim, addedValue, false );
			}
		}
		System.out.println( "DONE" );
	}

	// Multiply Matrix1 x Matrix2, put result in resultMatrix
	public void multiplyMatrices( int resultMatrix, int matrix1, int matrix2 ) throws SQLException 
	{
		// Variables
		String query;
		Statement statement;
		ResultSet resultSet;
		int M1_cols;
		int M1_rows;
		int M2_cols;
		int M2_rows;

		// Get Matrix 1 Dimensions
		query = "SELECT * FROM MATRIX WHERE MATRIX_ID = " + matrix1 + ";";
		statement = connection.createStatement();
		resultSet = statement.executeQuery( query );
		
		int numRows = 0;
		if( resultSet.last() ) {
		    numRows = resultSet.getRow();
		}
		
		if( numRows == 0 ){
			System.out.println( "ERROR" );
			return;    
		}

		M1_rows = resultSet.getInt( "ROW_DIM" );
		M1_cols = resultSet.getInt( "COL_DIM" );
		
		// Get Matrix 2 Dimensions
		query = "SELECT * FROM MATRIX WHERE MATRIX_ID = " + matrix2 + ";";
		statement = connection.createStatement();
		resultSet = statement.executeQuery( query );
	
		numRows = 0;
		if( resultSet.last() ) {
		    numRows = resultSet.getRow();
		}

		if( numRows == 0 ) {
		    	System.out.println( "ERROR" );
			return;
		}

		M2_rows = resultSet.getInt( "ROW_DIM" );
		M2_cols = resultSet.getInt( "COL_DIM" );

		// Check if they are Multiplication compatable
		if( M1_cols != M2_rows ) {
			System.out.println( "ERROR" );
			return;
		}
	
		// Delete current resultMatrix, create new one
		deleteMatrix( resultMatrix, false );
		query = "INSERT INTO MATRIX VALUES (" + resultMatrix + ", " + M1_rows + ", " + M2_cols + ");";
		statement = connection.createStatement();
		statement.executeUpdate( query );

		// Multiply Matrices
		for( int i = 1; i <= M1_rows; i++ ) {
		    for( int j = 1; j <= M2_cols; j++ ) {
			double cellValue = 0;
			for( int k = 1; k <= M1_cols; k++ ) {
				double M1_value = getValue( matrix1, i, k, false );
				double M2_value = getValue( matrix2, k, j, false );
				double multVal = M1_value * M2_value;
				cellValue += multVal;
			}
			setValue( resultMatrix, i, j, cellValue, false );
		    }
		}
		
		System.out.println( "DONE" );
		return;
	}

	// Transpose oldMatrix, put result in resultMatrix
	public void transposeMatrix( int resultMatrix, int oldMatrix ) throws SQLException 
	{
		// Variables
		String query;
		Statement statement;
		ResultSet resultSet;
		
		query = "SELECT * FROM MATRIX WHERE MATRIX_ID = " + oldMatrix + ";";
		statement = connection.createStatement();
		resultSet = statement.executeQuery( query );
		int numRows = 0;
		if( resultSet.last() ) {
			numRows = resultSet.getRow();
		}

		if( numRows == 0 ) {
		    	System.out.println( "ERROR" );
			return;
		}

		int oldRows = resultSet.getInt( "ROW_DIM" );
		int oldCols = resultSet.getInt( "COL_DIM" );
		
		// delete resultMatrix if it exists, and create new one
		deleteMatrix( resultMatrix, false );
		setMatrix( resultMatrix, oldCols, oldRows, false );		
		
		query = "SELECT * FROM MATRIX_DATA WHERE MATRIX_ID = " + oldMatrix + ";";
		statement = connection.createStatement();
		resultSet = statement.executeQuery( query );
		while( resultSet.next() ) {
			double value = resultSet.getDouble( "VALUE" );
			int row = resultSet.getInt( "ROW_NUM" );
			int col = resultSet.getInt( "COL_NUM" );
			setValue( resultMatrix, col, row, value, false );     
			
		}
		System.out.println( "DONE" );
		return;
	}
	
	// Executes SQL Query
	public void randomQuery( String query ) throws SQLException {
		
		// Variables
		Statement statement;
		ResultSet resultSet;
		
		statement = connection.createStatement();
		resultSet = statement.executeQuery( query );
		resultSet.first();
		System.out.println( resultSet.getString( 1 ) );
		return;
	} 

	// Main Function
	public static void main( String[] args ) throws ClassNotFoundException, SQLException
	{
		A3 a3 = new A3();
		a3.connection_string = args[ 0 ];
		a3.input_file = args[ 1 ];
		a3.connection = DriverManager.getConnection( a3.connection_string );
			
		try {
		    Scanner inFile = new Scanner( new FileReader ( a3.input_file ) );
		    String fileLine;
		    while( inFile.hasNextLine() ) {
			fileLine = inFile.nextLine();
			String [] words = fileLine.split( " " );
			if( words[ 0 ].equals( "SETM" ) ) {
				int index = Integer.parseInt( words[ 1 ] );
				int row_dim = Integer.parseInt( words[ 2 ] );
				int col_dim = Integer.parseInt( words[ 3 ] );
				a3.setMatrix( index, row_dim, col_dim, true );	
			} else if( words[ 0 ].equals( "GETV" ) ) {
			    	int index = Integer.parseInt( words[ 1 ] );
				int row_num = Integer.parseInt( words[ 2 ] );
				int col_num = Integer.parseInt( words[ 3 ] );
				a3.getValue( index, row_num, col_num, true );
			} else if( words[ 0 ].equals( "SETV" ) ) {
			    	int index = Integer.parseInt( words [ 1 ] );
				int row_num = Integer.parseInt( words[ 2 ] );
				int col_num = Integer.parseInt( words[ 3 ] );
				double value = Double.parseDouble( words[ 4 ] );
				a3.setValue( index, row_num, col_num, value, true );
			} else if( words[ 0 ].equals( ( "DELETE" ) ) ) {
			    	if( !( words[ 1 ].equals( "ALL" ) ) ) {
				    	int index = Integer.parseInt( words[ 1 ] );
				    	a3.deleteMatrix( index, true );
				} else {
					a3.deleteAll();		
				}
		    	} else if( words[ 0 ].equals( "ADD" ) ) {
				int matrix1 = Integer.parseInt( words[ 1 ] );
				int matrix2 = Integer.parseInt( words[ 2 ] );
				int matrix3 = Integer.parseInt( words[ 3 ] );
				a3.addSubtractMatrix( matrix1, matrix2, matrix3, true );
			} else if( words[ 0 ].equals( "SUB" ) ) {
				int matrix1 = Integer.parseInt( words[ 1 ] );
				int matrix2 = Integer.parseInt( words[ 2 ] );
				int matrix3 = Integer.parseInt( words[ 3 ] );
				a3.addSubtractMatrix( matrix1, matrix2, matrix3, false );
			} else if( words[ 0 ].equals( "MULT" ) ) {
				int matrix1 = Integer.parseInt( words[ 1 ] );
				int matrix2 = Integer.parseInt( words[ 2 ] );
				int matrix3 = Integer.parseInt( words[ 3 ] );
				a3.multiplyMatrices( matrix1, matrix2, matrix3 );
			} else if( words[ 0 ].equals( "TRANSPOSE" ) ) {
				int matrix1 = Integer.parseInt( words[ 1 ] );
				int matrix2 = Integer.parseInt( words[ 2 ] );
				a3.transposeMatrix( matrix1, matrix2 );	
			} else if( words[ 0 ].equals( "SQL" ) ) {
				String string = "";
				for( int i = 1; i < words.length; i++ ) {
					string = string + " " + words[ i ];
				}
				a3.randomQuery( string );		
			}	 
		    } // end while

		} catch( Exception e ) {
		    	System.out.println( e );
		}

	}
}
