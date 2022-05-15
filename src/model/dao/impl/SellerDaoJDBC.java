package model.dao.impl;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import db.DB;
import db.DbException;
import gui.util.Alerts;
import javafx.scene.control.Alert.AlertType;
import model.dao.SellerDao;
import model.entities.Department;
import model.entities.Seller;
import oracle.jdbc.OracleTypes;

public class SellerDaoJDBC implements SellerDao {
	
	private Connection conn;
	
	public SellerDaoJDBC(Connection conn) {
		this.conn = conn;
	}

	@Override
	public void insert(Seller obj) {
		
		CallableStatement cs = null;
		
		try {
			
			cs = conn.prepareCall("{call pkg_seller.prc_insere_seller(?,?,?,?,?,?,?)}");
			cs.registerOutParameter(1, OracleTypes.VARCHAR);
			cs.registerOutParameter(2, OracleTypes.VARCHAR);
			cs.registerOutParameter(3, OracleTypes.DATE);
			cs.registerOutParameter(4, OracleTypes.NUMBER);
			cs.registerOutParameter(5, OracleTypes.INTEGER);
			cs.registerOutParameter(6, OracleTypes.INTEGER);
			cs.registerOutParameter(7, OracleTypes.VARCHAR);
			
			cs.execute();
			
			cs.setString(1, obj.getName());
			cs.setString(2, obj.getEmail());
			cs.setDate(3, new Date(obj.getBirthDate().getTime()));
			cs.setDouble(4, obj.getBaseSalary());
			cs.setInt(5, obj.getDepartment().getId());
			cs.setInt(6, obj.getId());
			
			
//			obj.setName(cs.getString(1));
//			obj.setEmail(cs.getString(2));
//			obj.setBirthDate(new java.util.Date(cs.getDate(3).getTime()));
//			obj.setBaseSalary(cs.getDouble(4));
//			obj.getDepartment().setId(5);
//			obj.setId(6);
			
			String msgRetorno = cs.getString(7);
			
			if (msgRetorno.contains("ERRO") || msgRetorno.contains("ORA")) {
				Alerts.alerts("Erro ao Gravar o Registro", null, msgRetorno, AlertType.ERROR);
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
	public void update(Seller obj) {

		CallableStatement cs = null;
		
		try {

			cs = conn.prepareCall("{call pkg_seller.prc_atualiza_seller(?,?,?,?,?,?,?)}");
			cs.registerOutParameter(1, OracleTypes.INTEGER);
			cs.registerOutParameter(2, OracleTypes.VARCHAR);
			cs.registerOutParameter(3, OracleTypes.VARCHAR);
			cs.registerOutParameter(4, OracleTypes.DATE);
			cs.registerOutParameter(5, OracleTypes.NUMBER);
			cs.registerOutParameter(6, OracleTypes.INTEGER);
			cs.registerOutParameter(7, OracleTypes.VARCHAR);
			
			cs.execute();	
			
//			cs.setInt(1, obj.getId());
//			cs.setString(2, obj.getName());
//			cs.setString(3, obj.getEmail());
//			cs.setDate(4, obj.getBirthDate());
//			cs.setDouble(5, obj.getBaseSalary());
//			cs.setInt(6, obj.getDepartment().getId());
						
			String msgRetorno = cs.getString(7);
			
			if (msgRetorno.contains("Erro") || msgRetorno.contains("Ora")) {
				Alerts.alerts("Erro ao Atualizar o Registro", null, msgRetorno, AlertType.ERROR);
			}
			else {
				Alerts.alerts(null, null, msgRetorno, AlertType.INFORMATION);
			}
			
//			ps = conn.prepareStatement(
//					"update seller set "
//					+ "name = ?, "
//					+ "email = ?, "
//					+ "birthDate = ?, "
//					+ "baseSalary = ?, "
//					+ "departmentId = ? "
//					+ "where id = ?"
//					);
			
//			ps.setString(1, obj.getName());
//			ps.setString(2, obj.getEmail());
//			ps.setDate(3, new java.sql.Date(obj.getBirthDate().getTime()));
//			ps.setDouble(4, obj.getBaseSalary());
//			ps.setInt(5, obj.getDepartment().getId());
//			ps.setInt(6, obj.getId());
			
			
		}
		catch (SQLException e) {
			throw new DbException(e.getMessage());
		}
		finally {
			DB.closeCallableStatement(cs);
		}
		
	}

	@Override
	public void deleteById(Integer id) {

		CallableStatement cs = null;
		
		try {
			
			cs = conn.prepareCall("{call pkg_seller.prc_deleta_seller(?,?)}");
			cs.registerOutParameter(1, OracleTypes.INTEGER);
			cs.registerOutParameter(2, OracleTypes.VARCHAR);
			cs.execute();
						
			String msgRetorno = cs.getString(2);
			
			if (msgRetorno.contains("ERRO")) {
				Alerts.alerts("Erro ao Deletar o Registro", null, msgRetorno, AlertType.ERROR);
			}
			else {
				Alerts.alerts(null, null, msgRetorno, AlertType.INFORMATION);
			};
			
		}
		catch (SQLException e) {
			throw new DbException(e.getMessage());
			
		}
		finally {
			DB.closeCallableStatement(cs);
		}
	}

	@Override
	public Seller findById(Integer id) {
		
		PreparedStatement ps = null;
		ResultSet rs = null;
		
		try {
			
			ps = conn.prepareStatement("select seller.id, "
					+ "seller.name, "
					+ "seller.email, "
					+ "seller.birthDate, "
					+ "seller.baseSalary, "
					+ "seller.departmentId, "
					+ "d.name as depName "
					+ "from seller "
					+ "inner join department d on seller.departmentId = d.id "
					+ " where seller.Id = ?");
			
			// Parametros para a cláusula where
			ps.setInt(1, id);
			
			// Executa a consulta no banco
			rs = ps.executeQuery();
			
			if (rs.next()) {
				/*
				Seller obj = new Seller(rs.getInt("Id"), 
										rs.getString("name"), 
										rs.getString("email"),
										rs.getDate("birthDate"),
										rs.getDouble("baseSalary"), 
										new Department(rs.getInt("departmentId"), 
													   rs.getString("depName")));
				
				return obj;
				*/
				
				Department dep = instanciateDepartment(rs);
				Seller obj = instanciateSeller(rs, dep);
				return obj;
			}
			
			
		} 
		catch (SQLException e) {
		   throw new DbException(e.getMessage());
		}
		finally {
			DB.closeStatement(ps);
			DB.closeResultSet(rs);
		}

		return null;
	}

	private Seller instanciateSeller(ResultSet rs, Department dep) throws SQLException {
		Seller obj = new Seller(
				rs.getInt("id"), 
				rs.getString("name"), 
				rs.getString("email"),
				new java.util.Date(rs.getTimestamp("birthDate").getTime()),
				rs.getDouble("baseSalary"),
				dep);

		return obj;
	}

	private Department instanciateDepartment(ResultSet rs) throws SQLException {
		Department dep = new Department(
				rs.getInt("departmentId"),
				rs.getString("departmentName"));

		return dep;
	}

	@Override
	public List<Seller> findAll() {

		ResultSet rs = null;
		CallableStatement cs = null;
		
		try {
			
			cs = conn.prepareCall("{call pkg_seller.prc_retorna_tudo(?)}");
			cs.registerOutParameter(1, OracleTypes.REF_CURSOR);
			cs.execute();
			
			rs = (ResultSet) cs.getObject(1);
			List<Seller> listSeller = new ArrayList<>();
			Map<Integer, Department> map = new HashMap<Integer, Department>();
			
			while (rs.next()) {
				
				Department dep = map.get(rs.getInt("departmentId"));
				
				if (dep == null) {
					dep = instanciateDepartment(rs);
    				map.put(rs.getInt("departmentId"), dep);
    			}
				
				Seller obj = instanciateSeller(rs, dep);
				listSeller.add(obj);
			}
			
			return listSeller;
			
		} 
		catch (SQLException e) {
		   throw new DbException(e.getMessage());
		}
		finally {
			DB.closeResultSet(rs);
			DB.closeCallableStatement(cs);
		}

	}

	@Override
	public List<Seller> findByDepartment(Department department) {

		PreparedStatement ps = null;
		ResultSet rs = null;
		
		try {
			
			ps = conn.prepareStatement(
					"select seller.*, "
					+ "	 d.name as depName "
					+ "  from seller "
					+ "  join department d on seller.departmentId = d.id "
					+ " where d.id = ? "
					+ " order by name "
					);
			
			// Parametros para a cláusula where
			ps.setInt(1, department.getId());
			
			// Executa a consulta no banco
			rs = ps.executeQuery();
			
			List<Seller> listSeller = new ArrayList<Seller>();
			Map<Integer, Department> map = new HashMap<Integer, Department>();
			
			while (rs.next()) {
					
				Department dep = map.get(rs.getInt("departmentId"));

				if (dep == null) {
					dep = instanciateDepartment(rs);
					map.put(rs.getInt("departmentId"), dep);
				}
				
				Seller obj = instanciateSeller(rs, dep);
				listSeller.add(obj);
			}
			
			return listSeller;
			
		} 
		catch (SQLException e) {
		   throw new DbException(e.getMessage());
		}
		finally {
			DB.closeStatement(ps);
			DB.closeResultSet(rs);
		}


	}
	
}
