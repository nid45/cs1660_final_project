import javax.swing.*;
import javax.swing.filechooser.FileSystemView;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

public class GUI implements ActionListener {
    private JButton chooseFilesButton;
    private JButton constructInvertedIndicesButton;

    @Override
    public void actionPerformed(ActionEvent e) {

        if(e.getSource() == chooseFilesButton){
            JFileChooser j = new JFileChooser("d:", FileSystemView.getFileSystemView());

// Open the save dialog
            j.showSaveDialog(null);
        }
    }




}
