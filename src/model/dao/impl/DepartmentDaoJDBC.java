package model.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import db.DB;
import db.DbException;
import model.dao.DepartmentDao;
import model.entities.Department;

public class DepartmentDaoJDBC implements DepartmentDao {
	
	private Connection conn;
	
	public DepartmentDaoJDBC(Connection conn) {
		this.conn = conn;
	}

	@Override
	public void insert(Department obj) {

		PreparedStatement ps = null;
		
		try {
			conn.setAutoCommit(false);
			ps = conn.prepareStatement("insert into department (name) values (?)", Statement.RETURN_GENERATED_KEYS);
			ps.setString(1, obj.getName());
			
			int rowsAffected = ps.executeUpdate();
			
			if (rowsAffected > 0) {
				ResultSet rs = ps.getGeneratedKeys();
				if (rs.next()) {
					int id = rs.getInt(1);
					obj.setId(id);
				}
				DB.closeResultSet(rs);
			}
			
			if (rowsAffected == 0) {
				conn.rollback();
				throw new DbException("Nenhum departamento foi inserido");
			}
			else {
				conn.commit();
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
	public void update(Department obj) {
		
		PreparedStatement ps = null;
		
		try {
			
			conn.setAutoCommit(false);
			
			ps = conn.prepareStatement("update department set name = ? where id = ?");
			
			ps.setString(1, obj.getName());
			ps.setInt(2, obj.getId());
			
			int rowsAffected = ps.executeUpdate();
			
			if (rowsAffected == 0) {
				throw new DbException("Nenhum registro afetado!");
			} 
			else if (rowsAffected > 1) {
				conn.rollback();
				throw new DbException("Erro ao atualizar o registro!" + obj.getId());
			}
			else {
				conn.commit();
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
			
			ps = conn.prepareStatement("delete from department where id = ?");
			ps.setInt(1, id);
			
			int rowsAffected = ps.executeUpdate();
			
			if (rowsAffected == 0) {
				throw new DbException("Nenhum registro afetado!");
			} 
			else if (rowsAffected > 1) {
				conn.rollback();
				throw new DbException("Erro ao deletar o registro!");
			}
			else {
				conn.commit();
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
	public Department findById(Integer id) {

		PreparedStatement ps = null;
		ResultSet rs = null;
		
		try {
			
			ps = conn.prepareStatement("select * from department where id = ?");
			ps.setInt(1, id);
			
			rs = ps.executeQuery();
			
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
			DB.closeResultSet(rs);
			DB.closeStatement(ps);
		}
		
	}

	@Override
	public List<Department> findAll() {
		
		PreparedStatement ps = null;
		ResultSet rs = null;
		
		try {
			
			ps = conn.prepareStatement("select * from department");
			rs = ps.executeQuery();
			
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
