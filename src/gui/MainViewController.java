package gui;

import java.io.IOException;
import java.net.URL;
import java.util.ResourceBundle;

import application.Main;
import gui.util.Alerts;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.fxml.Initializable;
import javafx.scene.Node;
import javafx.scene.Scene;
import javafx.scene.control.Alert.AlertType;
import javafx.scene.control.MenuItem;
import javafx.scene.control.ScrollPane;
import javafx.scene.layout.VBox;

public class MainViewController implements Initializable {
	
	@FXML
	private MenuItem menuItemSeller;
	
	@FXML
	private MenuItem menuItemDepartment;
	
	@FXML
	private MenuItem menuItemAbout;

	@FXML
	public void onMenuItemSellerAction() {
		System.out.println("onMenuItemSellerAction");
	}
	
	@FXML
	public void onMenuItemDepartmentAction() {
		System.out.println("onMenuItemDepartmentAction");
	}

	@FXML
	public void onMenuItemAboutAction() {
		loadView("/gui/About.fxml");
	}
	
	public synchronized void loadView(String absoluteName) {
		
		try {
			
			// Carregando a tela filha. 
			FXMLLoader loader = new FXMLLoader(getClass().getResource(absoluteName));
			VBox newVbox = loader.load();
			
			/* Criando um objeto novo mainScene e criando um objeto VBox.
			 * Referenciando o scrollpane (principal) fazendo um casting de ScrollPane.
			 * Referenciando o VBox fazendo um casting de VBox
			 * */
			Scene mainScene = Main.getMainScene();
			VBox mainVBox = (VBox) ((ScrollPane) mainScene.getRoot()).getContent();
			
			// Criando um objeto Node recebendo o objeto filho na primeira posição do vetor.
			Node mainMenu = mainVBox.getChildren().get(0);
			mainVBox.getChildren().clear(); // Clear para limpar todos os filhos.
			mainVBox.getChildren().add(mainMenu); // add os novos filhos no node.
			mainVBox.getChildren().addAll(newVbox.getChildren()); // add uma coleção do objeto newVBox.
			
		} catch (IOException e) {
			Alerts.alerts("Error", null, e.getMessage(), AlertType.ERROR);
		}
		
		
	}
	
	@Override
	public void initialize(URL url, ResourceBundle rb) {			
	}

}
