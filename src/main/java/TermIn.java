import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.dataproc.Dataproc;
import com.google.api.services.dataproc.model.HadoopJob;
import com.google.api.services.dataproc.model.Job;
import com.google.api.services.dataproc.model.JobPlacement;
import com.google.api.services.dataproc.model.SubmitJobRequest;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.AuthCredentials;
import com.google.cloud.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import javax.swing.*;
import java.awt.*;
import java.awt.event.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;

public class TermIn extends JDialog {
    private JPanel contentPane;
    private JButton buttonOK;
    private JTextField textField1;
    private JButton buttonCancel;
    public static String term = "";
    public static int rand_int = MainGUI.rand_int;

    public TermIn() {
        setContentPane(contentPane);
        setModal(true);
        getRootPane().setDefaultButton(buttonOK);

        buttonOK.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                onOK();
            }
        });


        // call onCancel() when cross is clicked
        setDefaultCloseOperation(DO_NOTHING_ON_CLOSE);
        addWindowListener(new WindowAdapter() {
            public void windowClosing(WindowEvent e) {
                onCancel();
            }
        });

        // call onCancel() on ESCAPE
        contentPane.registerKeyboardAction(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                onCancel();
            }
        }, KeyStroke.getKeyStroke(KeyEvent.VK_ESCAPE, 0), JComponent.WHEN_ANCESTOR_OF_FOCUSED_COMPONENT);
    }

    private void onOK() {
        term = textField1.getText();
        StringBuffer output = new StringBuffer();
        //gcp project info
        String projectId = gcpvars.projectId;
        String cluster = gcpvars.cluster;

        //arg info for submitting job
        //input data folder
        String arg1 = gcpvars.arg1;
        //output folder
        String arg2 = gcpvars.arg2;
        String outputDir = "";
        File newDir = new File(System.getProperty("java.io.tmpdir"));
        Boolean success = false;
        File directory = new File("./datafolder/" + rand_int);
        if (directory.exists()) {
            System.out.println("Directory already exists ...");
        } else {
            System.out.println("Directory not exists, creating now");
            success = directory.mkdir();
            if (success) {
                System.out.printf("Successfully created new directory : %s%n", "./datafolder/" + rand_int);
            } else {
                System.out.printf("Failed to create new directory: %s%n", "./datafolder/" + rand_int);
            }
        }

        Path test = Paths.get("./datafolder/" + rand_int);

        try {
            //submit job to gcp
            //modified from https://stackoverflow.com/questions/35611770/how-do-you-use-the-google-dataproc-java-client-to-submit-spark-jobs-using-jar-fi
            //and discussion board posts
            InputStream inputStream = new FileInputStream("./credentials.json");
            GoogleCredentials credentials = GoogleCredentials.fromStream(inputStream)
                    .createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));
            HttpRequestInitializer requestInitializer = new HttpCredentialsAdapter(credentials);
            Dataproc dataproc = new Dataproc.Builder(new NetHttpTransport(), new JacksonFactory(), requestInitializer)
                    .setApplicationName("term-search")
                    .build();

            Job submittedJob = dataproc.projects().regions().jobs().submit(
                    projectId, "us-central1", new SubmitJobRequest()
                            .setJob(new Job()
                                    .setPlacement(new JobPlacement()
                                            .setClusterName(cluster))
                                    .setHadoopJob(new HadoopJob()
                                            .setMainClass("TermSearch")
                                            .setJarFileUris(ImmutableList.of(arg2+"JAR/TermSearch.jar"))
                                            .setArgs(ImmutableList.of(
                                                    arg2 + "output" + rand_int, arg2 + "output" + rand_int + term, term)))))
                    .execute();

            //wait for job to execute to move on
            //modified from https://stackoverflow.com/questions/35704048/what-is-the-best-way-to-wait-for-a-google-dataproc-sparkjob-in-java
            String jobId = submittedJob.getReference().getJobId();
            Job job = dataproc.projects().regions().jobs().get(projectId, "us-central1", jobId).execute();

            String status = job.getStatus().getState();
            while (!status.equalsIgnoreCase("DONE") && !status.equalsIgnoreCase("CANCELLED") && !status.equalsIgnoreCase("ERROR")) {
                System.out.println("Job not done yet; current state: " + job.getStatus().getState());
                Thread.sleep(5000);
                job = dataproc.projects().regions().jobs().get(projectId, "us-central1", jobId).execute();
                status = job.getStatus().getState();
            }

            System.out.println("Job terminated in state: " + job.getStatus().getState());
        } catch (Exception err) {
            err.printStackTrace();
        }

        //download files generated by above jobs
        //modified from https://stackoverflow.com/questions/25141998/how-to-download-a-file-from-google-cloud-storage-with-java
        //and https://cloud.google.com/storage/docs/listing-objects
        try {
            String bucketName = arg1;
            InputStream inputStream = new FileInputStream("./credentials.json");
            AuthCredentials credentials = AuthCredentials.createForJson(new FileInputStream("./credentials.json"));

            Storage storage = StorageOptions.builder().authCredentials(credentials).projectId(projectId).build().service();
            Bucket bucket = storage.get(bucketName);
            Page<Blob> blobs = (Page<Blob>) bucket.list(
                    Storage.BlobListOption.prefix(outputDir + "output" + rand_int + term));
            output.append(term + ":\n");
            for (Iterator<Blob> it = blobs.iterateAll(); it.hasNext(); ) {
                Blob blob = it.next();
                String blobContent = new String(blob.content()).replaceAll("(\\d)([A-Za-z])", "$1 $2");
                ;
                String[] removeTab = blobContent.split(" ");
                // String[] elements = removeTab[1].split(":");
                boolean bool = true;
                for (String temp : removeTab) {


                    if (temp.contains(":")) {
                        if (temp.contains("\t")) {
                            String[] first = temp.split("\t");
                            temp = first[1];
                        }
                        if (bool) {
                            bool = false;
                        }
                            String[] elements = temp.split(":");
                            output.append("Document: " + elements[0] + "  ");
                            output.append("Occurrences: " + elements[1] + "\n");

                    } else {
                    }
                }

            }
        } catch (Exception err) {
            err.printStackTrace();
        }


        JTextArea textArea = new JTextArea(output.toString());
        textArea.setEditable(false);
        JScrollPane scrollPane = new JScrollPane(textArea);
        textArea.setLineWrap(true);
        textArea.setWrapStyleWord(true);
        scrollPane.setPreferredSize(new Dimension(500, 500));
        JOptionPane.showMessageDialog(null, scrollPane, "Term Search Results", JOptionPane.YES_NO_OPTION);
        if (JOptionPane.showConfirmDialog(null, "return to options menu", "WARNING",
                JOptionPane.YES_NO_OPTION) == JOptionPane.YES_OPTION) {
            dispose();
            ChooseOption dialog = new ChooseOption();
            dialog.pack();
            dialog.setVisible(true);

        }
    }

    private void onCancel() {
        dispose();
    }

    public static void main(String[] args) {
        TermIn dialog = new TermIn();
        dialog.pack();
        dialog.setVisible(true);
        System.exit(0);
    }


}
