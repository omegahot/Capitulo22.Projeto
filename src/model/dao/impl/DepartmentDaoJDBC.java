package model.dao.impl;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import db.DB;
import db.DbException;
import gui.util.Alerts;
import javafx.scene.control.Alert.AlertType;
import model.dao.DepartmentDao;
import model.entities.Department;
import oracle.jdbc.OracleTypes;

public class DepartmentDaoJDBC implements DepartmentDao {
	
	private Connection conn;
	
	public DepartmentDaoJDBC(Connection conn) {
		this.conn = conn;
	}

	@Override
	public void insert(Department obj) {

		CallableStatement cs = null;
		ResultSet rs = null;

		
		try {
			conn.setAutoCommit(false);
			//ps = conn.prepareStatement("insert into department (name) values (?)", Statement.RETURN_GENERATED_KEYS);
			//ps = conn.prepareStatement("exec pkg_department.prc_insere_department (?, ?)");
			//ps.setString(1, obj.getName());
			
			cs = conn.prepareCall("{call pkg_department.prc_insere_department(?,?,?)}");
			cs.registerOutParameter(1, OracleTypes.VARCHAR);
			cs.registerOutParameter(2, OracleTypes.INTEGER);
			cs.registerOutParameter(3, OracleTypes.VARCHAR);
			cs.setString(1, obj.getName());

			cs.setString(1, obj.getName());
			cs.execute();

			obj.setId(cs.getInt(2));
			String msgRetorno = cs.getString(3);
			

			if (msgRetorno.contains("Atenção") || msgRetorno.contains("ORA")) {
				Alerts.alerts("Erro", null, msgRetorno, AlertType.ERROR);
			}
			
			else {
				Alerts.alerts(null, null, msgRetorno, AlertType.INFORMATION);
			}
			
		}
		catch (SQLException e) {
			throw new DbException(e.getMessage());
		} 
		finally {
			DB.closeCallableStatement(cs);
			DB.closeResultSet(rs);
		}
		
		
	}

	@Override
	public void update(Department obj) {
		
		CallableStatement cs = null;
		ResultSet rs = null;
		
		try {
			
			conn.setAutoCommit(false);
			
			//ps = conn.prepareStatement("update department set name = ? where id = ?");
			//ps = conn.prepareStatement("exec pkg_department.prc_atualiza_department (?, ?, ?)");
			cs = conn.prepareCall("{call pkg_department.prc_atualiza_department(?,?,?)}");
			cs.registerOutParameter(1, OracleTypes.INTEGER);
			cs.registerOutParameter(2, OracleTypes.VARCHAR);
			cs.registerOutParameter(3, OracleTypes.VARCHAR);
			cs.setInt(1, obj.getId());
			cs.setString(2, obj.getName());
			cs.execute();
			
			String msgRetorno = cs.getString(3);
			
			if (msgRetorno.contains("ERRO") || msgRetorno.contains("ORA")) {
				Alerts.alerts("Erro", null, msgRetorno, AlertType.ERROR);
			}
			
			else {
				Alerts.alerts(null, null, msgRetorno, AlertType.INFORMATION);
			} 
			
			
		}
		catch (SQLException e) {
			throw new DbException(e.getMessage());
		}
		finally {
			DB.closeCallableStatement(cs);
			DB.closeResultSet(rs);
		}
		
	}

	@Override
	public void deleteById(Integer id) {

		CallableStatement cs = null;
		
		try {
			conn.setAutoCommit(false);
			
			//ps = conn.prepareStatement("delete from department where id = ?");
			//ps.setInt(1, id);
			cs = conn.prepareCall("{call pkg_department.prc_deleta_department(?,?)}");
			cs.registerOutParameter(1, OracleTypes.INTEGER);
			cs.registerOutParameter(2, OracleTypes.VARCHAR);
			cs.setInt(1, id);
			cs.execute();
			
			String msgRetorno = cs.getString(2);

			if (msgRetorno.contains("ERRO") || msgRetorno.contains("ORA")) {
				Alerts.alerts("Erro", null, msgRetorno, AlertType.ERROR);
			}
			
			else {
				Alerts.alerts(null, null, msgRetorno, AlertType.INFORMATION);
			} 
			
			
		}
		catch (SQLException e) {
			throw new DbException(e.getMessage());
		}
		finally {
			DB.closeCallableStatement(cs);
		}
	}

	@Override
	public Department findById(Integer id) {

		CallableStatement cs = null;
		ResultSet rs = null;
		
		try {
			
			//ps = conn.prepareStatement("select * from department where id = ?");
			//ps.setInt(1, id);
			cs = conn.prepareCall("{call pkg_department.prc_retorna_department(?,?)}");
			cs.registerOutParameter(1, OracleTypes.INTEGER);
			cs.registerOutParameter(2, OracleTypes.REF_CURSOR);
			cs.setInt(1, id);
			cs.execute();
			
			rs = (ResultSet) cs.getObject(1);
			
			if (rs.next()) {
				Department dep = new Department(
						rs.getInt("id"), 
						rs.getString("name"));
							
				return dep;	
			}
			
			return null;
			
			
		}
		catch (SQLException e) {
			throw new DbException(e.getMessage());
		} 
		finally {
			DB.closeCallableStatement(cs);
			DB.closeResultSet(rs);
			
		}
		
	}

	@Override
	public List<Department> findAll() {
		
		PreparedStatement ps = null;
		CallableStatement cs = null;
		ResultSet rs = null;
		
		try {
			
			//ps = conn.prepareStatement("select * from department");
			
			cs = conn.prepareCall("{call pkg_department.prc_retorna_tudo(?)}");
			cs.registerOutParameter(1, OracleTypes.REF_CURSOR);
			cs.execute();
			rs = (ResultSet) cs.getObject(1);
			
			List<Department> list = new ArrayList<Department>();
			
			while (rs.next()) {
				
				list.add(new Department(
						rs.getInt("id"),
						rs.getString("name")));
				
			}
			
			return list;
			
		}
		catch (SQLException e) {
			throw new DbException(e.getMessage());
		} 
		finally {
			DB.closeResultSet(rs);
			DB.closeStatement(ps);
		}

	}

}
