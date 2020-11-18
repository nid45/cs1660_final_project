

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
import com.google.cloud.storage.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.utils.FileNameUtils;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

import javax.swing.*;
import javax.swing.filechooser.FileSystemView;
import java.awt.*;
import java.awt.event.*;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.zip.GZIPInputStream;


public class MainGUI extends JDialog {
    private JPanel contentPane;
    private JButton buttonOK;
    private JButton buttonCancel;
    private JList list1;
    private JScrollPane scrollPane;
    private static DefaultListModel filenames = new DefaultListModel();
    private static List<File> files = new ArrayList<File>();
    public static Random rand = new Random();
    public static int rand_int = rand.nextInt(1000000000);


    public MainGUI() {
        setContentPane(contentPane);
        setModal(true);
        getRootPane().setDefaultButton(buttonOK);


        buttonOK.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                try {
                    onOK();
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                }
            }
        });

        buttonCancel.addActionListener(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                try {
                    onCancel();
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                }
            }
        });

        // call onCancel() when cross is clicked
        setDefaultCloseOperation(DO_NOTHING_ON_CLOSE);
        addWindowListener(new WindowAdapter() {
            public void windowClosing(WindowEvent e) {
                try {
                    onCancel();
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                }
            }
        });

        // call onCancel() on ESCAPE
        contentPane.registerKeyboardAction(new ActionListener() {
            public void actionPerformed(ActionEvent e) {
                try {
                    onCancel();
                } catch (IOException ioException) {
                    ioException.printStackTrace();
                }
            }
        }, KeyStroke.getKeyStroke(KeyEvent.VK_ESCAPE, 0), JComponent.WHEN_ANCESTOR_OF_FOCUSED_COMPONENT);
    }

    private File onOK() throws IOException {
        // add your code here
        JFileChooser j = new JFileChooser("d:", FileSystemView.getFileSystemView());

// Open the save dialog
        int result = j.showOpenDialog(null);

        // if user clicked Cancel button on dialog, return
        if (result == JFileChooser.CANCEL_OPTION)
            System.exit(1);

        File fileName = j.getSelectedFile(); // get selected file

        // display error if invalid
        if ((fileName == null) || (fileName.getName().equals(""))) {
            JOptionPane.showMessageDialog(this, "Invalid File Name",
                    "Invalid File Name", JOptionPane.ERROR_MESSAGE);
            System.exit(1);
        } // end if


        InputStream inputStream = new FileInputStream("./credentials.json");
        GoogleCredentials credentials = GoogleCredentials.fromStream(inputStream)
                .createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));
        HttpRequestInitializer requestInitializer = new HttpCredentialsAdapter(credentials);
        filenames.addElement(fileName.getName());
        list1.setModel(filenames);
        Dataproc dataproc = new Dataproc.Builder(new NetHttpTransport(), new JacksonFactory(), requestInitializer)
                .setApplicationName("topn").build();


        files.add(fileName);

        return fileName;
    }


    private static void unTarGz(String tarFile, File destFile) {
        TarArchiveInputStream tis = null;
        try {
            FileInputStream fis = new FileInputStream(tarFile);
            // .gz
            GZIPInputStream gzipInputStream = new GZIPInputStream(new BufferedInputStream(fis));
            //.tar.gz
            tis = new TarArchiveInputStream(gzipInputStream);
            TarArchiveEntry tarEntry = null;
            while ((tarEntry = tis.getNextTarEntry()) != null) {
                System.out.println(" tar entry- " + tarEntry.getName());
                if (tarEntry.isDirectory()) {
                    continue;
                } else {
                    // In case entry is for file ensure parent directory is in place
                    // and write file content to Output Stream
                    File outputFile = new File(destFile + File.separator + tarEntry.getName());
                    outputFile.getParentFile().mkdirs();
                    IOUtils.copy(tis, new FileOutputStream(outputFile));
                }
            }
        } catch (IOException ex) {
            System.out.println("Error while untarring a file- " + ex.getMessage());
        } finally {
            if (tis != null) {
                try {
                    tis.close();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
    }


    static void uploadFolder(File[] files, String folder, String bucket, Storage storage) throws IOException {
        for (File file : files) {
            if (!file.isHidden()) {
                // if it is a directory read the files within the subdirectory
                if (file.isDirectory()) {
                    String[] lpath = file.getAbsolutePath().split("/");
                    String lfolder = lpath[lpath.length - 1];
                    String xfolder = folder + "/" + lfolder;
                    uploadFolder(file.listFiles(), xfolder, bucket, storage); // Calls same method again.

                } else {
                    // add directory/subdirectory to the file name to create the file structure
                    BlobId blobId = BlobId.of(bucket, "Data/" + rand_int + "/" + file.getName());

                    //prepare object
                    BlobInfo blobInfo = BlobInfo.builder(blobId).build();

                    // upload object
                    storage.create(blobInfo, Files.readAllBytes(Paths.get(file.getAbsolutePath())));
                    System.out.println("Uploaded: gs://" + bucket + "/Data/" + rand_int + "/" + file.getName());
                }

            }

        }
    }

    private void onCancel() throws IOException {
        StringBuffer output = new StringBuffer();
        //gcp project info
        String projectId = "bustling-vim-294815";
        String cluster = "hadoop";

        //arg info for submitting job
        //input data folder
        String arg1 = "dataproc-staging-us-central1-17322103279-q89zhjwt";
        //output folder
        String arg2 = "gs://dataproc-staging-us-central1-17322103279-q89zhjwt/";
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

        // OutputStream outputStream = new OutputStream();


        for (int i = 0; i <= files.size() - 1; i++) {

            FileUtils.copyFile(files.get(i), new File("./datafolder/" + rand_int + "/" + files.get(i).getName()));

            //if (files.get(i).getName().substring(files.get(i).getName().length() - 3, files.get(i).getName().length()) == ".gz") {
            unTarGz("./datafolder/" + rand_int + "/" + files.get(i).getName(), new File("./datafolder/" + rand_int + "/"));
            FileUtils.forceDelete(new File("./datafolder/" + rand_int + "/" + files.get(i).getName()));
            //}

            AuthCredentials authCredentials = AuthCredentials.createForJson(new FileInputStream("./credentials.json"));
            Storage storage = (Storage) StorageOptions.builder().projectId(projectId).authCredentials(authCredentials).build().service();

            BlobId blobId = BlobId.of(arg1, "Data/" + rand_int + "/" + files.get(i).getName());
            BlobInfo blobInfo = BlobInfo.builder(blobId).build();
            String[] filesplit = files.get(i).getName().split("\\.");

            File[] dir = new File("./datafolder/" + rand_int + "/").listFiles();
            uploadFolder(dir, filesplit[0], arg1, storage);


            //   ((Storage) storage).create(blobInfo, Files.readAllBytes(Paths.get("./datafolder/" + rand_int + "/" + filesplit[0])));


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
                    .setApplicationName("inverted-index")
                    .build();

            Job submittedJob = dataproc.projects().regions().jobs().submit(
                    projectId, "us-central1", new SubmitJobRequest()
                            .setJob(new Job()
                                    .setPlacement(new JobPlacement()
                                            .setClusterName(cluster))
                                    .setHadoopJob(new HadoopJob()
                                            .setMainClass("InvertedIndex")
                                            .setJarFileUris(ImmutableList.of("gs://dataproc-staging-us-central1-17322103279-q89zhjwt/JAR/Inverted.jar"))
                                            .setArgs(ImmutableList.of(
                                                    "gs://" + arg1 + "/Data/" + rand_int, arg2 + "output" + rand_int)))))
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
            String bucketName = "dataproc-staging-us-central1-17322103279-q89zhjwt";
            InputStream inputStream = new FileInputStream("./credentials.json");
            AuthCredentials credentials = AuthCredentials.createForJson(new FileInputStream("./credentials.json"));

            Storage storage = StorageOptions.builder().authCredentials(credentials).projectId(projectId).build().service();
            Bucket bucket = storage.get(bucketName);
            Page<Blob> blobs = bucket.list(
                    Storage.BlobListOption.prefix(outputDir + "output" + rand_int));
            for (Iterator<Blob> it = blobs.iterateAll(); it.hasNext(); ) {
                Blob blob = (Blob) it.next();
                String blobContent = new String(blob.content());
                output.append(blobContent);
            }
        } catch (Exception err) {
            err.printStackTrace();
        }

        //print out inverted indices on UI using output generated above
        if (output.length() == 0) {
            output.append("Oops! Please select at least one of the options to construct an inverted index.");
        }

        JTextArea textArea = new JTextArea(output.toString());
        textArea.setEditable(false);
        JScrollPane scrollPane = new JScrollPane(textArea);
        textArea.setLineWrap(true);
        textArea.setWrapStyleWord(true);
        scrollPane.setPreferredSize(new Dimension(500, 500));
        JOptionPane.showMessageDialog(null, scrollPane, "Inverted Index Results", JOptionPane.YES_NO_OPTION);

        dispose();
        ChooseOption dialog = new ChooseOption();
        dialog.pack();
        dialog.setVisible(true);
    }

    public static void main(String[] args) {

        MainGUI dialog = new MainGUI();
        dialog.pack();
        dialog.setVisible(true);

        //System.exit(0);
    }

    public void setData(List<String> data) {
        try {
            for (int i = 0; i < filenames.size() - 1; i++) {

                JTextArea textArea = new JTextArea(filenames.toString());
                textArea.setEditable(false);
                scrollPane = new JScrollPane(textArea);
                textArea.setLineWrap(true);
                textArea.setWrapStyleWord(true);
                scrollPane.setPreferredSize(new Dimension(500, 500));
            }
        } catch (Exception e) {

        }

    }

    private void createUIComponents() {


        JTextArea textArea = new JTextArea(filenames.get(filenames.size() - 1).toString());
        textArea.setEditable(false);
        scrollPane = new JScrollPane(textArea);
        textArea.setLineWrap(true);
        textArea.setWrapStyleWord(true);
        scrollPane.setPreferredSize(new Dimension(500, 500));
    }



}
