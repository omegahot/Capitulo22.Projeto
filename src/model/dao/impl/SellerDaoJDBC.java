package model.dao.impl;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import db.DB;
import db.DbException;
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
		
		PreparedStatement ps = null;
		
		try {
			
			ps = conn.prepareStatement (
					"insert into seller (name, email, birthDate, baseSalary, departmentId) "
					+ "values "
					+ "(?, ?, ?, ?, ?) ", Statement.RETURN_GENERATED_KEYS);
			
			ps.setString(1, obj.getName());
			ps.setString(2, obj.getEmail());
			ps.setDate(3, new java.sql.Date(obj.getBirthDate().getTime()));
			ps.setDouble(4, obj.getBaseSalary());
			ps.setInt(5, obj.getDepartment().getId());
			
			int rowsAffected = ps.executeUpdate();
			
			if (rowsAffected > 0) {
				ResultSet rs = ps.getGeneratedKeys();
				if (rs.next()) {
					int id = rs.getInt(1);
					obj.setId(id);
				} 
				DB.closeResultSet(rs);
			}
			
		} 
		catch (SQLException e) {
			throw new DbException(e.getMessage());
		} 
		finally {
			DB.closeStatement(ps);
		}
		
	}

	@Override
	public void update(Seller obj) {

		PreparedStatement ps = null;
		
		try {
			conn.setAutoCommit(false);
			
			ps = conn.prepareStatement(
					"update seller set "
					+ "name = ?, "
					+ "email = ?, "
					+ "birthDate = ?, "
					+ "baseSalary = ?, "
					+ "departmentId = ? "
					+ "where id = ?"
					);
			
			ps.setString(1, obj.getName());
			ps.setString(2, obj.getEmail());
			ps.setDate(3, new java.sql.Date(obj.getBirthDate().getTime()));
			ps.setDouble(4, obj.getBaseSalary());
			ps.setInt(5, obj.getDepartment().getId());
			ps.setInt(6, obj.getId());
			
			int rowsAffect = ps.executeUpdate();
			
			if (rowsAffect == 1) {
				conn.commit();
				
			} else {
				conn.rollback();
				throw new DbException("Linhas afetadas acima do esperado.");
				
			}
			
		}
		catch (SQLException e) {
			throw new DbException(e.getMessage());
		}
		finally {
			DB.closeStatement(ps);
		}
		
	}

	@Override
	public void deleteById(Integer id) {

		PreparedStatement ps = null;
		try {
			
			conn.setAutoCommit(false);
			
			ps = conn.prepareStatement(
					"delete from seller "
					+ "where id = ? "
					);
			
			ps.setInt(1, id);
			
			int rowsAffected = ps.executeUpdate();
			
			if (rowsAffected == 1 ) {
				conn.commit();
			}
			else {
				conn.rollback();
				throw new DbException("Erro ao deletar o registro!!");
			}
			
		}
		catch (SQLException e) {
			throw new DbException(e.getMessage());
			
		}
		finally {
			DB.closeStatement(ps);
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
				rs.getInt("Id"), 
				rs.getString("name"), 
				rs.getString("email"),
				rs.getDate("birthDate"),
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
