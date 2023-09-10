package edu.lowell.lig.joe;

import java.awt.geom.AffineTransform;
import java.awt.geom.Point2D;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.BindException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.jms.JMSException;
import javax.jms.Message;

import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.equinox.app.IApplication;
import org.eclipse.equinox.app.IApplicationContext;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.simpleframework.xml.Serializer;
import org.simpleframework.xml.core.Persister;
import org.simpleframework.xml.stream.Format;

import TCSDataDefinitions.TCSCommand.GuiderCommands.Guide;
import TCSDataDefinitions.TCSCommand.GuiderCommands.GuideStateTypes;
import TCSDataDefinitions.TCSCommand.OffsetCommands.OffsetTypeValues;
import TCSDataDefinitions.TCSCommand.OffsetCommands.ScienceTargetOffset;
import TCSDataDefinitions.TCSCommand.TargetConfiguration.ScienceTargetConfiguration;
import TCSDataDefinitions.TCSGuiderStatus.GuideTargetPosition;
import TCSDataDefinitions.TCSPublicStatus.MountGuideModeTypes;
import TCSDataDefinitions.TCSPublicStatus.NewScienceTarget;
import TCSDataDefinitions.TCSPublicStatus.TCSTcsStatus;
import TCSDataDefinitions.TCSWFSStatus.WavefrontTargetPosition;
import edu.lowell.lig.common.dct.model.FacilitySummary;
import edu.lowell.lig.common.dct.model.FoldMirrorState;
import edu.lowell.lig.common.dct.model.GwavesGenericCommand;
import edu.lowell.lig.common.dct.model.GwavesStageCommand;
import edu.lowell.lig.common.dct.model.InstrumentCoverState;
import edu.lowell.lig.common.dct.model.OMSCard;
import edu.lowell.lig.common.dct.model.OMSCardAxis;
import edu.lowell.lig.common.dct.model.ProbeName;
import edu.lowell.lig.common.dct.model.ProbeState;
import edu.lowell.lig.common.dct.model.ScienceTarget;
import edu.lowell.lig.common.dct.model.WfsAccumulatorCommand;
import edu.lowell.lig.common.dct.utils.HelperUtilities;
import edu.lowell.lig.common.dct.utils.OMSCardUtilities;
import edu.lowell.lig.common.model.ErrorLevel;
import edu.lowell.lig.common.model.ErrorMessage;
import edu.lowell.lig.common.utils.ApplicationConstants;
import edu.lowell.lig.common.utils.DateTimeMatcher;
import edu.lowell.lig.common.utils.JodaTime;
import edu.lowell.lig.common.utils.LoggerUtilities;
import edu.lowell.lig.jms.core.MessageListenerForRCP;
import edu.lowell.lig.jms.core.Messenger;
import edu.lowell.lig.jms.utils.BrokerConstants;
import edu.lowell.lig.jms.utils.BrokerTopicNames;
import edu.lowell.lig.jms.utils.JmsUtilities;
import edu.lowell.lig.joe.listener.GuiderDeltasMessageListener;
import edu.lowell.lig.joe.listener.LoisScriptCommandStageRouter;
import edu.lowell.lig.joe.listener.LoisScriptCommandTcsRouter;
import edu.lowell.lig.joe.listener.MsgListenerDevenyOffset;
import edu.lowell.lig.joe.listener.PduCommandRouter;
import edu.lowell.lig.joe.listener.StageCommandRouter;
import edu.lowell.lig.joe.model.LoisCommand;
import edu.lowell.lig.joe.utils.JdbcUtils;
import edu.lowell.loui.logging.LogSelector;

/**
 * This class controls all aspects of the application's execution
 */
public class Application implements IApplication {
  /**
   * Logger for this class
   */
  private static final Logger logger = LogManager
      .getLogger(Activator.PLUGIN_ID + LogSelector.getLogger());

  private static final int CHECK_PORT = 9999;
  private static ServerSocket checkSocket;

  private static Map<String, String> tcsData = new HashMap<String, String>();

  private static ScienceTarget scienceTarget;
  private static double scienceTrackId;
  private static ScienceTargetConfiguration scienceTargetConfiguration;
  private static boolean offsetCommand;
  private static boolean slewCommand;
  private static long offsetTimestamp;
  private static double offset;

  private static Map<ProbeName, ProbeState> probeStatus = new HashMap<ProbeName, ProbeState>();
  private static Map<ProbeName, Boolean> probeAutoFocusStatus = new HashMap<ProbeName, Boolean>();
  private static Map<ProbeName, Float> probeAutoFocusScaleFactor = new HashMap<ProbeName, Float>();
  private static Map<ProbeState, double[]> probeFocalPlaneCoordinates = new HashMap<ProbeState, double[]>();
  private static Map<ProbeName, double[]> probeStageCoordinates = new HashMap<ProbeName, double[]>();
  private static List<String> loisProducerTopics = new ArrayList<String>();
  private static List<String> gwavesProducerTopics = new ArrayList<String>();

  private static int heartbeatPeriod;
  private static int heartbeatDelay;
  private static int readoutPeriod;
  private static int readoutDelay;
  //D.M.Lytle - We remove various instruments from this list when they are out of service.
  private static String cardListToStart = ",RC1,RC2,OMS3,OMS4,DEVENY";
  //private static String cardListToStart = ",OMS3,OMS4,DEVENY";
  private static List<GwavesGenericCommand> stageReadoutCommandList = new ArrayList<GwavesGenericCommand>();
  private static List<OMSCard> omsCardList = new ArrayList<OMSCard>();
  private static Map<String, LoisCommand> loisCommandMap = new HashMap<String, LoisCommand>();
  private static Map<String, String> axisFunctionMap = new HashMap<String, String>();
  private static Map<String, String> instrumentTopicMap = new HashMap<String, String>();
  private static boolean xStageCommand = false;
  private static boolean yStageCommand = false;
  private static boolean stageSlip = false;
  private static boolean stageLimit = false;

  //  public static boolean errorMessageSent = false;
  private static boolean stageReadoutRestart = false;
  private static boolean joeHeartbeatRestart = true;

  public static InstrumentCoverState icState = InstrumentCoverState.UNKNOWN;
  private static double icStageCoordinate;
  public static Map<String, FoldMirrorState> fmStateMap = new HashMap<String, FoldMirrorState>();
  public static Map<String, Double> fmStageCoordinates = new HashMap<String, Double>();
  private static Map<String, Boolean> fmComplexHomeMap = new HashMap<String, Boolean>();

  private static boolean[] lmiFilterWheelDetent = { false, false };
  private static int[] lmiFilterWheelPosition = { 0, 0 };

  private static double[] wfsAccumulator = new double[48];

  private static FacilitySummary facSum;
  private static double aosFocusOffset;

  private static boolean guideOnChip = false;

  //  private static ScheduledExecutorService dctStatusScheduler;
  //  private static Runnable dctStatusMonitor;
  
  private static StageCommandRouter stageCommandRouter;    // Lytle

  static {
    axisFunctionMap.put("AX", "STAGE");
    axisFunctionMap.put("AY", "STAGE");
    axisFunctionMap.put("AZ", "FOCUS");
    axisFunctionMap.put("AT", "FILTER");

    instrumentTopicMap.put("RC1", BrokerTopicNames.JOE_OMS_RC1_COMMAND_RESULT);
    instrumentTopicMap.put("RC2", BrokerTopicNames.JOE_OMS_RC2_COMMAND_RESULT);
    instrumentTopicMap.put("OMS3", BrokerTopicNames.JOE_OMS_LMI_COMMAND_RESULT);
    instrumentTopicMap.put("OMS4", BrokerTopicNames.JOE_OMS_LMI_COMMAND_RESULT);

    // Default values for probe status & probeFocalPlaneCoordinates
    probeStatus.put(ProbeName.RC2, ProbeState.GDR);
    probeStatus.put(ProbeName.RC1, ProbeState.WFS);
    probeFocalPlaneCoordinates.put(ProbeState.GDR, new double[2]);
    probeFocalPlaneCoordinates.put(ProbeState.WFS, new double[2]);
    probeStageCoordinates.put(ProbeName.RC1, new double[2]);
    probeStageCoordinates.put(ProbeName.RC2, new double[2]);
    probeAutoFocusStatus.put(ProbeName.RC1, Boolean.TRUE);
    probeAutoFocusStatus.put(ProbeName.RC2, Boolean.TRUE);

    // Initializing tcsData
    tcsData.put(ApplicationConstants.TCS_IMAGE_PARALLACTIC_ANGLE, "0.0");
    tcsData.put(ApplicationConstants.TCS_ROTATOR_POSITION_ANGLE, "0.0");
    tcsData.put(ApplicationConstants.MOUNT_GUIDE_MODE,
        MountGuideModeTypes.NoTrack.toString());
    tcsData.put(ApplicationConstants.DITHER_TOLERANCE, "0.0");

    for (String s : ApplicationConstants.FOLD_MIRROR_FUNCTIONS) {
      fmStateMap.put(s, FoldMirrorState.UNKNOWN);
      fmStageCoordinates.put(s, Double.MAX_VALUE);
    }
    for (String s : ApplicationConstants.COMPLEX_HOME_PROBE_FUNCTIONS) {
      fmComplexHomeMap.put(s, Boolean.FALSE);

    }
  }

  /* (non-Javadoc)
   * @see org.eclipse.equinox.app.IApplication#start(org.eclipse.equinox.app.IApplicationContext)
   */
  public Object start(IApplicationContext context) throws Exception {
    if (logger.isTraceEnabled()) {
      logger
          .trace("start(IApplicationContext context=" + context + ") - start");
    }
    JmsUtilities.setPluginId(Activator.PLUGIN_ID);
    JmsUtilities.setInstrumentId("joe");

    // Let's check to make sure another instance of JOE is not running
    // on this machine.
    checkIfRunning();
    System.out.println(edu.lowell.lig.common.utils.HelperUtilities
        .getApplicationLogAnnouncement("JOE"));

    logger.warn(edu.lowell.lig.common.utils.HelperUtilities
        .getApplicationLogAnnouncement("JOE"));

    // Checking to see whether the message broker is running or not. 
    // If not, we'll get the connection URL from the properties file
    // and let the user know.
    String warningText = Messenger.testBroker();
    if (warningText != null) {
      logger
          .error("There seems to be a problem connecting to the Message Broker");
      logger.error(warningText);

      if (logger.isTraceEnabled()) {
        logger.trace("start(IApplicationContext) - end");
      }
      return IApplication.EXIT_OK;
    }

    // Checking to see if the time is synchronized properly
    warningText = JodaTime.testNtpTimeOffset();
    if (warningText != null) {
      logger.warn(warningText);
      System.out.println(warningText);
    }

    // Starting the TCS-JOE message routing...
    // Getting the instrument configuration file which has the list
    // of the topic names
    XMLConfiguration config = JmsUtilities.getInstrumentConfiguration();
    String prefix = "";
    if (JmsUtilities.getInstrumentId() != null) {
      prefix = JmsUtilities.getInstrumentId() + ".";
    }
    heartbeatPeriod = config.getInt(prefix + "heartbeat-period", 5000); // default period: 5 seconds
    heartbeatDelay = 100; // default delay: 0.1 second
    readoutPeriod = config.getInt(prefix + "readout-period", 2000); // default period: 2 seconds
    readoutDelay = config.getInt(prefix + "readout-delay", 1000); // default delay: 1 second

    int numTopics = 0;
    Object prop = config
        .getProperty(prefix + "lois-producer-topics.topic-name");
    if (prop instanceof Collection) {
      numTopics = ((Collection<?>) prop).size();
    } else {
      numTopics = 1;
    }
    for (int i = 0; i < numTopics; i++) {
      String baseKey = prefix + "lois-producer-topics.topic-name(" + i + ")";
      loisProducerTopics.add(config.getString(baseKey));
    }
    prop = config.getProperty(prefix + "gwaves-producer-topics.topic-name");
    if (prop instanceof Collection) {
      numTopics = ((Collection<?>) prop).size();
    } else {
      numTopics = 1;
    }
    for (int i = 0; i < numTopics; i++) {
      String baseKey = prefix + "gwaves-producer-topics.topic-name(" + i + ")";
      gwavesProducerTopics.add(config.getString(baseKey));
    }

    probeAutoFocusScaleFactor.put(ProbeName.RC1,
        config.getFloat("rcprobe.aos-auto-focus.rc1", 0.0113f));
    probeAutoFocusScaleFactor.put(ProbeName.RC2,
        config.getFloat("rcprobe.aos-auto-focus.rc2", 0.0113f));

    //    omsCardList = HelperUtilities.getOmsCardList();
    //    if (logger.isDebugEnabled()) {
    //      logger.debug("OMS Card List: " + omsCardList);
    //    }

    // Starting the telemetry broadcast
    (new LoisTelemetryPublisher()).start();

    // Starting the TCS-JOE Message filtering
    (new TCSMessageJOEPublisher()).start();

    // Starting the ActiveMQ Log Monitor
    (new FileMonitor()).start();

    // Adding the message listeners
    for (int i = 0; i < loisProducerTopics.size(); i++) {
      String fullTopicName = JmsUtilities.generateFullTopicName(
          loisProducerTopics.get(i), ProbeName.RC1.toString());
      Messenger.addDistributedMessageListener(fullTopicName,
          new LOISMessageRouterListener(fullTopicName), getClass().getName(),
          false);

      fullTopicName = JmsUtilities.generateFullTopicName(
          loisProducerTopics.get(i), ProbeName.RC2.toString());
      Messenger.addDistributedMessageListener(fullTopicName,
          new LOISMessageRouterListener(fullTopicName), getClass().getName(),
          false);
    }
    for (int i = 0; i < gwavesProducerTopics.size(); i++) {
      String fullTopicName = JmsUtilities.generateFullTopicName(
          gwavesProducerTopics.get(i), ProbeState.GDR.toString());
      Messenger.addDistributedMessageListener(fullTopicName,
          new GWAVESMessageRouterListener(fullTopicName), getClass().getName(),
          false);

      fullTopicName = JmsUtilities.generateFullTopicName(
          gwavesProducerTopics.get(i), ProbeState.WFS.toString());
      Messenger.addDistributedMessageListener(fullTopicName,
          new GWAVESMessageRouterListener(fullTopicName), getClass().getName(),
          false);
    }

    Messenger.addDistributedMessageListener(
        BrokerTopicNames.TCS_PUBLIC_STATUS_QUEUE,
        new TcsPublicStatusListener(), getClass().getName(), false);

    Messenger.addDistributedMessageListener(
        BrokerTopicNames.TCS_GUIDER_STATUS_QUEUE, new GuiderStatusListener(),
        getClass().getName(), false);

    Messenger.addDistributedMessageListener(
        BrokerTopicNames.TCS_WFS_STATUS_QUEUE,
        new WavefrontSensorStatusListener(), getClass().getName(), false);

    Messenger.addDistributedMessageListener(
        BrokerTopicNames.JOE_STAGE_READOUT_STATUS, new StageReadoutListener(),
        getClass().getName(), false);

    Messenger.addDistributedMessageListener(
        BrokerTopicNames.JOE_HEARTBEAT_STATUS,
        new JoeHeartbeatStatusListener(), getClass().getName(), false);

    Messenger.addDistributedMessageListener(BrokerTopicNames.JOE_OMS_COMMAND,
        new LoisScriptCommandStageRouter(loisCommandMap), getClass().getName(),
        false);

    Messenger.addDistributedMessageListener(BrokerTopicNames.JOE_TCS_COMMAND,
        new LoisScriptCommandTcsRouter(loisCommandMap), getClass().getName(),
        false);

    Messenger.addDistributedMessageListener(BrokerTopicNames.AOS_SETTLED,
        new MsgListenerAosSettled(), getClass().getName(), false);

    Messenger.addDistributedMessageListener(
        BrokerTopicNames.AOS_RELATIVE_FOCUS_OFFSET_TOPIC,
        new MsgListenerAosRelativeFocusOffset(), getClass().getName(), false);

    Messenger.addDistributedMessageListener(
        BrokerTopicNames.TCS_IN_POSITION_QUEUE, new MsgListenerTcsInPosition(),
        getClass().getName(), false);

    Messenger.addDistributedMessageListener(
        BrokerTopicNames.JOE_WFS_ACCUMULATOR_COMMAND_TOPIC,
        new MsgListenerWFSAccumuator(), getClass().getName(), false);

    Messenger.addDistributedMessageListener(
        BrokerTopicNames.JOE_STAGE_RESULT_QUEUE, new MsgListenerStageResult(),
        getClass().getName(), false);

//    StageCommandRouter stageCommandRouter = new StageCommandRouter();
    stageCommandRouter = new StageCommandRouter();     // Lytle
    Messenger.addDistributedMessageListener(BrokerTopicNames.JOE_STAGE_QUEUE,
        stageCommandRouter, getClass().getName(), false);
    omsCardList = stageCommandRouter.getOmsCardList();
    if (omsCardList.size() == 0) {
      omsCardList = HelperUtilities.getOmsCardList();
    }
    if (logger.isDebugEnabled()) {
      logger.debug("OMS Card List: " + omsCardList);
    }

    Messenger.addDistributedMessageListener(BrokerTopicNames.JOE_PDU_QUEUE,
        new PduCommandRouter(), getClass().getName(), false);

    Messenger.addDistributedMessageListener(BrokerTopicNames.JOE_COMMAND,
        new MsgListenerJoeCommand(), getClass().getName(), false);

    Messenger.addDistributedMessageListener(BrokerTopicNames.JOE_REQUEST,
        new MsgListenerJoeRequest(), getClass().getName(), false);

    Messenger.addDistributedMessageListener(
        BrokerTopicNames.TCS_SCIENCE_TARGET_QUEUE,
        new MsgListenerScienceTarget(), getClass().getName(), false);

    Messenger.addDistributedMessageListener(BrokerTopicNames.TCS_COMMAND_TOPIC,
        new MsgListenerTcsCommand(), getClass().getName(), false);

    Messenger.addDistributedMessageListener(BrokerTopicNames.JOE_GUIDER_DELTAS,
        new GuiderDeltasMessageListener(), getClass().getName(), false);

    // Send messages to start the heartbeat timer & stage readout
    Messenger.sendDistributedMessage(BrokerTopicNames.JOE_HEARTBEAT_STATUS,
        BrokerConstants.START, false);
    Messenger.sendDistributedMessage(BrokerTopicNames.JOE_STAGE_READOUT_STATUS,
        BrokerConstants.START + cardListToStart, false);

    Messenger.addDistributedMessageListener(
        BrokerTopicNames.JOE_GUIDER_ACTIONS, new MsgListenerGuiderActions(),
        getClass().getName(), false);

    Messenger.addDistributedMessageListener(BrokerTopicNames.JOE_DEVENY_OFFSET,
        new MsgListenerDevenyOffset(), getClass().getName(), false);

    // Adding a dctStatusScheduler to update the database for the show command
    Runnable dctStatusMonitor = new Runnable() {
      public void run() {
        if (logger.isTraceEnabled()) {
          logger.trace("run() - start");
        }

        Map<String, String> dataMap = new HashMap<String, String>();
        if (facSum != null) {
          dataMap.put("az", facSum.getCurrentAz());
          dataMap.put("el", facSum.getCurrentEl());
          dataMap.put("ra", facSum.getDemandRA().toString());
          dataMap.put("dec", facSum.getDemandDec().toString());
          dataMap.put("targname", facSum.getTargetName());
          dataMap.put("guidemode", facSum.getMountGuideMode());
          dataMap.put("m1coverstate", facSum.getM1CoverState());
          dataMap.put("airmass", facSum.getAirmass());
          dataMap.put("parang", facSum.getCurrentParAngle());
          dataMap.put("rotpa", facSum.getCurrentRotPA());
          dataMap.put("rotiaa", facSum.getCurrentRotIAA());
          dataMap.put("rotipa", facSum.getCurrentRotIPA());
          //          dataMap.put("rotframe", facSum.getRotatorFrame());
          dataMap.put("rotframe", scienceTargetConfiguration.getRotator()
              .getRotatorFrame().toString());
        }
        for (String s : ApplicationConstants.FOLD_MIRROR_FUNCTIONS) {
          dataMap.put(s.toLowerCase(), fmStateMap.get(s).toString());
        }
        dataMap.put("focusoffset",
            ApplicationConstants.minuteFormatter.format(aosFocusOffset));
        dataMap.put("rc2status", probeStatus.get(ProbeName.RC2).toString());
        dataMap.put("rc1status", probeStatus.get(ProbeName.RC1).toString());
        dataMap.put("ic", icState.toString());
        dataMap.put("gdrfocalplane",
            Arrays.toString(probeFocalPlaneCoordinates.get(ProbeState.GDR)));
        dataMap.put("wfsfocalplane",
            Arrays.toString(probeFocalPlaneCoordinates.get(ProbeState.WFS)));
        dataMap.put(ProbeName.RC1.toString(), probeStatus.get(ProbeName.RC1)
            .toString());
        dataMap.put(ProbeName.RC2.toString(), probeStatus.get(ProbeName.RC2)
            .toString());
        dataMap.put("lmifws", edu.lowell.lig.common.dct.utils.HelperUtilities
            .generateLMIFilterNames(lmiFilterWheelPosition));
        dataMap.put("lmilfw", edu.lowell.lig.common.dct.utils.HelperUtilities
            .getLMIFilterNames(lmiFilterWheelPosition)[0]);
        dataMap.put("lmiufw", edu.lowell.lig.common.dct.utils.HelperUtilities
            .getLMIFilterNames(lmiFilterWheelPosition)[1]);

        try {
          JdbcUtils.updateKeywords(dataMap);
        } catch (SQLException e) {
          logger.error("run()", e); //$NON-NLS-1$
        }

        if (logger.isTraceEnabled()) {
          logger.trace("run() - end");
        }
      }
    };
    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    scheduler.scheduleAtFixedRate(dctStatusMonitor, 1000, 30000,
        TimeUnit.MILLISECONDS);

    synchronized (this) {
      this.wait();
    }

    if (logger.isTraceEnabled()) {
      logger.trace("start(IApplicationContext) - end - return value=" +
          IApplication.EXIT_OK);
    }
    return IApplication.EXIT_OK;
  }

  /* (non-Javadoc)
   * @see org.eclipse.equinox.app.IApplication#stop()
   */
  public void stop() {
    if (logger.isTraceEnabled()) {
      logger.trace("stop() - start");
    }

    // TODO - What can we put here? Do we need any thing here?

    if (logger.isTraceEnabled()) {
      logger.trace("stop() - end");
    }
  }

  private void checkIfRunning() {
    try {
      //Bind to localhost adapter with a zero connection queue 
      checkSocket = new ServerSocket(CHECK_PORT, 0,
          InetAddress.getByAddress(new byte[] { 127, 0, 0, 1 }));
      StringBuilder sb = new StringBuilder();
      sb.append("--> ")
          .append(ISODateTimeFormat.dateTime().print(new DateTime()))
          .append(ApplicationConstants.CR);
      sb.append("--> JOE is bound to: ").append(checkSocket);
      System.out.println(sb.toString());
      if (logger.isInfoEnabled()) {
        logger.info(sb.toString());
      }
    } catch (BindException e) {
      System.err
          .println("An instance of JOE is already running on this machine.");
      System.exit(1);
    } catch (IOException e) {
      System.err.println("Ooops. Unexpected Error.");
      e.printStackTrace();
      System.exit(2);
    }
  }

  private void sendCheckDetentCommand(OMSCardAxis axis) {
    GwavesGenericCommand stageCommand = new GwavesGenericCommand();
    stageCommand.setAxis(axis);
    stageCommand.composeCommand("BX");
    try {
      Messenger.sendDistributedMessage(BrokerTopicNames.JOE_STAGE_QUEUE,
          stageCommand.toXml(), false);
      if (logger.isInfoEnabled()) {
        logger.info(LoggerUtilities.generateBrokerSendLogMessage(
            BrokerTopicNames.JOE_STAGE_QUEUE, stageCommand.toXml()));
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static String getProbeName(ProbeState probeState) {
    String result = ProbeName.UNKNOWN.toString();

    Iterator<Entry<ProbeName, ProbeState>> it = probeStatus.entrySet()
        .iterator();
    while (it.hasNext()) {
      Map.Entry<ProbeName, ProbeState> entry = it.next();
      if (entry.getValue() == probeState) {
        result = entry.getKey().toString();
      }
    }

    return result;
  }

  public static ProbeState getProbeState(ProbeName probeName) {
    return probeStatus.get(probeName);
  }

  /**
   * @return the scienceTarget
   */
  public static ScienceTarget getScienceTarget() {
    return scienceTarget;
  }

  public static double getScienceTrackId() {
    return scienceTrackId;
  }

  public static boolean[] getLmiFilterWheelDetent() {
    return lmiFilterWheelDetent;
  }

  public static int[] getLmiFilterWheelPosition() {
    return lmiFilterWheelPosition;
  }

  /**
   * This listener is responsible for redirecting messages that are produced by
   * two GWAVES LOISes (RC1 & RC2) to the logical topics (GDR & WFS) depending
   * on the state of each probe.
   * 
   * @author szoonem
   */
  private class LOISMessageRouterListener extends MessageListenerForRCP {
    /**
     * Logger for this class
     */
    private final Logger logger = LogManager.getLogger(Activator.PLUGIN_ID +
        LogSelector.getLogger());

    private String originTopicName;
    private ProbeName probeName;

    /**
     * @param name
     *          has to in the form of LOUI.rc1.loisCommand
     */
    public LOISMessageRouterListener(String name) {
      super();
      if (logger.isTraceEnabled()) {
        logger.trace("LOISMessageRouterListener(String name=" + name +
            ") - start");
      }
      String[] params = name.split(BrokerConstants.TOPIC_NAME_DELIMETER_REGEX);
      this.probeName = ProbeName.valueOf(StringUtils.upperCase(params[1]));
      this.originTopicName = params[2];

      if (logger.isTraceEnabled()) {
        logger.trace("LOISMessageRouterListener(String, int) - end");
      }
    }

    public void run() {
      if (logger.isTraceEnabled()) {
        logger.trace("run() - start");
      }
      Message msg = message.removeLast();
      boolean bmt = bytesMessageType.removeLast();
      // Figuring out where to redirect the message
      String fullDestinationTopicName = JmsUtilities.generateFullTopicName(
          originTopicName, probeStatus.get(probeName).toString());

      if (bmt) {
        Messenger.sendDistributedMessage(fullDestinationTopicName,
            getByteArray(msg), false);
      } else {
        Messenger.sendDistributedMessage(fullDestinationTopicName,
            getMessageString(msg), false);
      }

      if (logger.isDebugEnabled()) {
        logger.debug(LoggerUtilities.generateBrokerSendLogMessage(
            fullDestinationTopicName, getMessageString(msg)));
      }

      if (logger.isTraceEnabled()) {
        logger.trace("run() - end");
      }
    }
  }

  /**
   * This listener is responsible for redirecting command messages that are
   * produced by two GWAVES LOUIs (GDR & WFS) to the hardware topics (rc1 & rc2)
   * depending on the state of each probe. <br/>
   * For now we only have one such topic: loisCommand.
   * 
   * @author szoonem
   */
  private class GWAVESMessageRouterListener extends MessageListenerForRCP {
    /**
     * Logger for this class
     */
    private final Logger logger = LogManager.getLogger(Activator.PLUGIN_ID +
        LogSelector.getLogger());

    private String originTopicName;
    private ProbeState probeState;

    /**
     * @param name
     *          has to in the form of LOUI.GDR.loisCommand
     */
    public GWAVESMessageRouterListener(String name) {
      super();
      if (logger.isTraceEnabled()) {
        logger.trace("GWAVESMessageRouterListener(String name=" + name +
            ") - start");
      }
      String[] params = name.split(BrokerConstants.TOPIC_NAME_DELIMETER_REGEX);
      this.probeState = ProbeState.valueOf(params[1]);
      this.originTopicName = params[2];

      if (logger.isTraceEnabled()) {
        logger.trace("GWAVESMessageRouterListener(String, int) - end");
      }
    }

    public void run() {
      if (logger.isTraceEnabled()) {
        logger.trace("run() - start");
      }
      Message msg = message.removeLast();
      String msgString = getMessageString(msg);
      if (msgString == null) {
        return;
      }

      if (logger.isInfoEnabled()) {
        try {
          logger.info(LoggerUtilities.generateBrokerReceiveLogMessage(
              getTopic(msg), msgString, msg.getJMSMessageID()));
        } catch (JMSException e) {
          logger.error("run()", e); //$NON-NLS-1$
        }
      }

      // Figuring out where to redirect the message
      String fullDestinationTopicName = JmsUtilities.generateFullTopicName(
          originTopicName, getProbeName(probeState));

      Messenger.sendDistributedMessage(fullDestinationTopicName,
          getMessageString(msg), false);

      if (logger.isInfoEnabled()) {
        logger.info(LoggerUtilities.generateBrokerSendLogMessage(
            fullDestinationTopicName, msgString));
      }
      if (logger.isTraceEnabled()) {
        logger.trace("run() - end");
      }
    }
  }

  private class TcsPublicStatusListener extends MessageListenerForRCP {
    /**
     * Logger for this class
     */
    private final Logger logger = LogManager.getLogger(Activator.PLUGIN_ID +
        LogSelector.getLogger());

    public void run() {
      if (logger.isTraceEnabled()) {
        logger.trace("run() - start");
      }

      Message msg = message.removeLast();
      try {
        // Deserializing
        Serializer serializer = new Persister(new DateTimeMatcher(),
            new Format(2));
        TCSTcsStatus tcsPublicStatus = serializer.read(TCSTcsStatus.class,
            getMessageString(msg));
        // Let's get all the interesting info from FacilitySummary class which 
        // is a lot easier to access
        facSum = new FacilitySummary(tcsPublicStatus);

        tcsData.put(ApplicationConstants.TCS_ROTATOR_POSITION_ANGLE,
            facSum.getCurrentRotPA());
        tcsData.put(ApplicationConstants.TCS_INSTRUMENT_ALIGNMENT_ANGLE,
            facSum.getCurrentRotIAA());
        tcsData.put(ApplicationConstants.TCS_ROTATOR_INSTRUMENT_POSITION_ANGLE,
            facSum.getCurrentRotIPA());
        tcsData.put(ApplicationConstants.TCS_IMAGE_PARALLACTIC_ANGLE,
            facSum.getCurrentParAngle());
        tcsData.put(ApplicationConstants.MOUNT_GUIDE_MODE,
            facSum.getMountGuideMode());
        tcsData.put(ApplicationConstants.TCS_CURRENT_RA, facSum.getCurrentRA()
            .toString());
        tcsData.put(ApplicationConstants.TCS_CURRENT_DEC, facSum
            .getCurrentDec().toString());

        if (logger.isDebugEnabled()) {
          StringBuilder sb = new StringBuilder();
          sb.append("tcsData (TCS Status): ");
          sb.append(ApplicationConstants.TCS_ROTATOR_POSITION_ANGLE)
              .append("=").append(facSum.getCurrentRotPA());
          sb.append(",");
          sb.append(ApplicationConstants.TCS_INSTRUMENT_ALIGNMENT_ANGLE)
              .append("=").append(facSum.getCurrentRotIAA());
          sb.append(",");
          sb.append(ApplicationConstants.TCS_ROTATOR_INSTRUMENT_POSITION_ANGLE)
              .append("=").append(facSum.getCurrentRotIPA());
          sb.append(",");
          sb.append(ApplicationConstants.TCS_IMAGE_PARALLACTIC_ANGLE)
              .append("=").append(facSum.getCurrentParAngle());
          sb.append(",");
          sb.append(ApplicationConstants.MOUNT_GUIDE_MODE).append("=")
              .append(facSum.getMountGuideMode());
          sb.append(",");
          sb.append(ApplicationConstants.TCS_CURRENT_RA).append("=")
              .append(facSum.getCurrentRA().toString());
          sb.append(",");
          sb.append(ApplicationConstants.TCS_CURRENT_DEC).append("=")
              .append(facSum.getCurrentDec().toString());
          logger.debug(sb.toString());
        }

        // Sending info to the light path view
        Messenger.sendDistributedMessage(BrokerTopicNames.LIGHT_PATH_INFO,
            edu.lowell.lig.common.utils.HelperUtilities.generateNameValueInfo(
                ApplicationConstants.MIRROR_COVER_MODE,
                facSum.getM1CoverState()), false);
        Messenger
            .sendDistributedMessage(BrokerTopicNames.LIGHT_PATH_INFO,
                edu.lowell.lig.common.utils.HelperUtilities
                    .generateNameValueInfo(
                        ApplicationConstants.INSTRUMENT_COVER_STATE,
                        icState.toString()), false);
        Messenger.sendDistributedMessage(BrokerTopicNames.LIGHT_PATH_INFO,
            edu.lowell.lig.common.utils.HelperUtilities.generateNameValueInfo(
                ApplicationConstants.INSTRUMENT_COVER_STAGE_COORDINATE,
                ApplicationConstants.angleErrorFormatter
                    .format(icStageCoordinate)), false);
        StringBuilder tmp = new StringBuilder();
        for (String s : ApplicationConstants.FOLD_MIRROR_FUNCTIONS) {
          tmp.append(fmStateMap.get(s)).append(
              ApplicationConstants.COORDINATE_DELIMITER);
        }
        Messenger.sendDistributedMessage(BrokerTopicNames.LIGHT_PATH_INFO,
            edu.lowell.lig.common.utils.HelperUtilities.generateNameValueInfo(
                ApplicationConstants.FOLD_MIRRORS_STATE,
                tmp.substring(0, tmp.length() - 1)), false);
        tmp = new StringBuilder();
        for (String s : ApplicationConstants.FOLD_MIRROR_FUNCTIONS) {
          tmp.append(
              ApplicationConstants.angleErrorFormatter
                  .format(fmStageCoordinates.get(s))).append(
              ApplicationConstants.COORDINATE_DELIMITER);
        }
        Messenger.sendDistributedMessage(BrokerTopicNames.LIGHT_PATH_INFO,
            edu.lowell.lig.common.utils.HelperUtilities.generateNameValueInfo(
                ApplicationConstants.FOLD_MIRRORS_STAGE_COORDINATES,
                tmp.substring(0, tmp.length() - 1)), false);
      } catch (Exception e) {
        logger.error("run()", e);
      }

      if (logger.isTraceEnabled()) {
        logger.trace("run() - end");
      }
    }
  }

  /**
   * This listener redirects the message to the device listener which is either
   * RC1.probeStatus or RC2.probeStatus
   * 
   * @author szoonem
   */
  private class GuiderStatusListener extends MessageListenerForRCP {
    /**
     * Logger for this class
     */
    private final Logger logger = LogManager.getLogger(Activator.PLUGIN_ID +
        LogSelector.getLogger());

    public void run() {
      if (logger.isTraceEnabled()) {
        logger.trace("run() - start");
      }

      Message msg = message.removeLast();
      GuideTargetPosition tcsGuideTargetPosition = new GuideTargetPosition();
      double[] xyCoordinate = new double[2];

      // We will deserialize the XML and update probeFocalPlaneCoordinates
      Serializer serializer = new Persister(new DateTimeMatcher(),
          new Format(2));
      try {
        tcsGuideTargetPosition = serializer.read(GuideTargetPosition.class,
            getMessageString(msg));
        xyCoordinate[0] = tcsGuideTargetPosition.getTarget().getxPosition_mm();
        xyCoordinate[1] = tcsGuideTargetPosition.getTarget().getyPosition_mm();
        probeFocalPlaneCoordinates.put(ProbeState.GDR, xyCoordinate);
      } catch (Exception e) {
        logger.error("run()", e);
      }

      // Getting the affine transform from the config file for the probe
      // and applying it to the focal plane coordinates to obtain the 
      // stage coordinates for the specified probe.
      AffineTransform transformer = HelperUtilities
          .getProbeAffineTransform(StringUtils
              .lowerCase(getProbeName(ProbeState.GDR)));
      Point2D ptSrc = new Point2D.Double(xyCoordinate[0], xyCoordinate[1]);
      Point2D ptDst = HelperUtilities.applyAffineTransform(transformer, ptSrc);

      probeStageCoordinates.put(
          ProbeName.valueOf(getProbeName(ProbeState.GDR)),
          new double[] { ptDst.getX(), ptDst.getY() });

      // TODO - Do we want to redierct the message to rc1.probeStatus or
      // rc2.probeStatus? Do we use Probe or ProbeTelemetry objects?
      //      ProbeTelemetry targetTelemetry = new ProbeTelemetry(
      //          tcsGuideTargetPosition);
      //      Messenger.sendDistributedMessage(getProbeName(ProbeState.GDR) +
      //          BrokerConstants.TOPIC_NAME_DELIMETER + "probeStatus",
      //          targetTelemetry.toXml(), false);

      if (logger.isTraceEnabled()) {
        logger.trace("run() - end");
      }
    }

  }

  /**
   * This listener redirects the message to the device listener which is either
   * RC1.probeStatus or RC2.probeStatus
   * 
   * @author szoonem
   */
  private class WavefrontSensorStatusListener extends MessageListenerForRCP {
    /**
     * Logger for this class
     */
    private final Logger logger = LogManager.getLogger(Activator.PLUGIN_ID +
        LogSelector.getLogger());

    public void run() {
      if (logger.isTraceEnabled()) {
        logger.trace("run() - start");
      }

      Message msg = message.removeLast();
      WavefrontTargetPosition tcsWavefrontTargetPosition = new WavefrontTargetPosition();
      double[] xyCoordinate = new double[2];

      // We will deserialize the XML and update probeFocalPlaneCoordinates
      Serializer serializer = new Persister(new DateTimeMatcher(),
          new Format(2));
      try {
        tcsWavefrontTargetPosition = serializer.read(
            WavefrontTargetPosition.class, getMessageString(msg));
        xyCoordinate[0] = tcsWavefrontTargetPosition.getTarget()
            .getxPosition_mm();
        xyCoordinate[1] = tcsWavefrontTargetPosition.getTarget()
            .getyPosition_mm();
        probeFocalPlaneCoordinates.put(ProbeState.WFS, xyCoordinate);
      } catch (Exception e) {
        logger.error("run() - " + ApplicationConstants.CR +
            getMessageString(msg), e);
      }
      // TODO -- test this--
      // Getting the affine transform from the config file for the probe
      // and applying it to the focal plane coordinates to obtain the 
      // stage coordinates for the specified probe.
      AffineTransform transformer = HelperUtilities
          .getProbeAffineTransform(StringUtils
              .lowerCase(getProbeName(ProbeState.WFS)));
      Point2D ptSrc = new Point2D.Double(xyCoordinate[0], xyCoordinate[1]);
      Point2D ptDst = HelperUtilities.applyAffineTransform(transformer, ptSrc);

      probeStageCoordinates.put(
          ProbeName.valueOf(getProbeName(ProbeState.WFS)),
          new double[] { ptDst.getX(), ptDst.getY() });

      // TODO - Do we want to redierct the message to rc1.probeStatus or
      // rc2.probeStatus? Do we use Probe or ProbeTelemetry objects?
      //      ProbeTelemetry targetTelemetry = new ProbeTelemetry(
      //          tcsWavefrontTargetPosition);
      //
      //      Messenger.sendDistributedMessage(getProbeName(ProbeState.WFS) +
      //          BrokerConstants.TOPIC_NAME_DELIMETER + "probeStatus",
      //          targetTelemetry.toXml(), false);

      if (logger.isTraceEnabled()) {
        logger.trace("run() - end");
      }
    }

  }

  /**
   * This listener turns on or off the Stage Readout Command Possible commands
   * are: start,autorestart=true stop,autorestart=false start stop
   * 
   * @author szoonem
   */
  private class StageReadoutListener extends MessageListenerForRCP {
    /**
     * Logger for this class
     */
    private final Logger logger = LogManager.getLogger(Activator.PLUGIN_ID +
        LogSelector.getLogger());

    private boolean alreadyRunning = false;
    private Timer readoutTimer;

    public void run() {
      if (logger.isTraceEnabled()) {
        logger.trace("run() - start");
      }
      Message msg = message.removeLast();
      String msgString = getMessageString(msg);
      if (msgString == null) {
        return;
      }

      StringBuilder sb = new StringBuilder();
      sb.append(ApplicationConstants.CR).append(new DateTime())
          .append(ApplicationConstants.CR);
      sb.append("----StageReadoutListener----start").append(
          ApplicationConstants.CR);
      sb.append("READ-OUT Restart: " + stageReadoutRestart).append(
          ApplicationConstants.CR);
      try {
        sb.append(msgString).append("|").append(msg.getJMSMessageID())
            .append(ApplicationConstants.CR);
      } catch (JMSException e) {
        logger.warn("run() - exception ignored", e); //$NON-NLS-1$
      }

      if (msgString.contains(BrokerConstants.START)) {
        if (!alreadyRunning) {
          sb.append("Got a message to start the READ-OUT Timer").append(
              ApplicationConstants.CR);

          if (msgString.equals(BrokerConstants.START)) {
            msgString += cardListToStart;
          } else {
            cardListToStart = msgString.substring(msgString.indexOf(","));
          }

          // Scheduler for issuing the RP command to the OMS Card
          sb.append("Starting Stage READ-OUT Timer.....").append(
              ApplicationConstants.CR);
          // Generate the list of the cards that we are going to command
          stageReadoutCommandList = new ArrayList<GwavesGenericCommand>();
          for (OMSCard card : omsCardList) {
            if (cardListToStart.contains(card.getName())) {
              OMSCardAxis axis = card.getAxisList().get(0);
              GwavesGenericCommand stageCommand = new GwavesGenericCommand();
              stageCommand.setAxis(axis);
              if (card.getName().contains("RC") ||
                  card.getName().contains("DEVENY")) {
                stageCommand
                    .setCommand(ApplicationConstants.ALL_AXES_REPORT_ENCODER);
              } else {
                stageCommand
                    .setCommand(ApplicationConstants.ALL_AXES_REPORT_POSITION);
              }
              stageReadoutCommandList.add(stageCommand);
            }
          }
          readoutTimer = new Timer();
          readoutTimer.scheduleAtFixedRate(new TimerTask() {
            public void run() {
              if (logger.isTraceEnabled()) {
                logger.trace("run() - start");
              }
              /*              
                            // Generate the list of the cards that we are going to command
                            stageReadoutCommandList = new ArrayList<GwavesGenericCommand>();
                            for (OMSCard card : omsCardList) {
                              if (cardListToStart.contains(card.getName())) {
                                OMSCardAxis axis = card.getAxisList().get(0);
                                GwavesGenericCommand stageCommand = new GwavesGenericCommand();
                                stageCommand.setAxis(axis);
                                stageCommand
                                    .setCommand(ApplicationConstants.ALL_AXES_REPORT_POSITION);
                                stageReadoutCommandList.add(stageCommand);
                              }
                            }
              */
              for (GwavesGenericCommand stageCommand : stageReadoutCommandList) {
                try {
                  Messenger.sendDistributedMessage(
                      BrokerTopicNames.JOE_STAGE_QUEUE, stageCommand.toXml(),
                      false);
                } catch (Exception e1) {
                  logger.error("run()", e1);
                }
              }

              if (logger.isTraceEnabled()) {
                logger.trace("run() - end");
              }
            }
          }, readoutDelay, readoutPeriod);
          alreadyRunning = true;
          stageReadoutRestart = true;
        } else {
          // The Timer is already running. Ignore and continue...
          sb.append(
              "Got a message to start the Stage READ-OUT Timer but the timer is already running....")
              .append(ApplicationConstants.CR);
        }
      } else if (BrokerConstants.STOP.equals(msgString)) {
        sb.append("Got a message to stop the READ-OUT Timer").append(
            ApplicationConstants.CR);
        // We need to make sure that timer is running before stopping it.
        // Otherwise a NPE will be thrown.
        if (alreadyRunning) {
          readoutTimer.cancel();
          alreadyRunning = false;
        } else {
          // The readout was not running when the stop command was received.
          // So we should set the status to false so it would not start
          // automatically.
          stageReadoutRestart = false;
          sb.append(
              "Stage READ-OUT Timer was not running when STOP command arrived. Restart:")
              .append(stageReadoutRestart).append(ApplicationConstants.CR);
        }
      } else {
        // This should not happen...
        logger.error("Bad Command Arriving at " +
            BrokerTopicNames.JOE_STAGE_READOUT_STATUS + " topic." +
            ApplicationConstants.CR + msgString);
      }
      sb.append("----StageReadoutListener----end");
      if (logger.isInfoEnabled()) {
        logger.info(sb.toString());
      }
      if (logger.isTraceEnabled()) {
        logger.trace("run() - end");
      }
    }
  }

  /**
   * This listener turns on or off the JOE heartbeat timer
   * 
   * @author szoonem
   */
  private class JoeHeartbeatStatusListener extends MessageListenerForRCP {
    /**
     * Logger for this class
     */
    private final Logger logger = LogManager.getLogger(Activator.PLUGIN_ID +
        LogSelector.getLogger());

    private boolean alreadyRunning = false;
    private Timer heartbeatTimer;

    public void run() {
      if (logger.isTraceEnabled()) {
        logger.trace("run() - start");
      }
      Message msg = message.removeLast();
      String msgString = getMessageString(msg);
      if (msgString == null) {
        return;
      }

      StringBuilder sb = new StringBuilder();
      sb.append(ApplicationConstants.CR).append(new DateTime())
          .append(ApplicationConstants.CR);
      sb.append("----JoeHeartbeatStatusListener----start").append(
          ApplicationConstants.CR);
      sb.append("HEARTBEAT Restart: ").append(joeHeartbeatRestart)
          .append(ApplicationConstants.CR);
      try {
        sb.append(msgString).append("|").append(msg.getJMSMessageID())
            .append(ApplicationConstants.CR);
      } catch (JMSException e) {
        logger.warn("run() - exception ignored", e); //$NON-NLS-1$
      }

      if (msgString.contains(BrokerConstants.START)) {
        if (!alreadyRunning) {
          sb.append("Got a message to start the HEARTBEAT Timer").append(
              ApplicationConstants.CR);

          // Sending the heart beat to the topic
          sb.append("Starting JOE Heartbeat Timer.....").append(
              ApplicationConstants.CR);
          heartbeatTimer = new Timer();
          heartbeatTimer.scheduleAtFixedRate(new TimerTask() {
            public void run() {
              if (logger.isTraceEnabled()) {
                logger.trace("run() - start");
              }

              try {
                Messenger.sendDistributedMessage(
                    BrokerTopicNames.JOE_HEARTBEAT, "on", false);
              } catch (Exception e1) {
                logger.error(
                    "run() - Problem sending hearbeat. Aborting the timer...",
                    e1);
                heartbeatTimer.cancel();
              }

              if (logger.isTraceEnabled()) {
                logger.trace("run() - end");
              }
            }
          }, heartbeatDelay, heartbeatPeriod);
          alreadyRunning = true;
          joeHeartbeatRestart = true;
          sb.append("HEARTBEAT Restart: ").append(joeHeartbeatRestart)
              .append(ApplicationConstants.CR);
        } else {
          // The Timer is already running. Ignore and continue...
          sb.append(
              "Got a message to start the HEARTBEAT Timer but the timer is already running....")
              .append(ApplicationConstants.CR);
        }
      } else if (BrokerConstants.STOP.equals(msgString)) {
        sb.append("Got a message to stop the HEARTBEAT Timer").append(
            ApplicationConstants.CR);
        // We need to make sure that timer is running before stopping it.
        // Otherwise a NPE will be thrown.
        if (alreadyRunning) {
          heartbeatTimer.cancel();
          alreadyRunning = false;
        } else {
          // The timer was not running when the stop command was received.
          // So we should set the status to false so it would not start
          // automatically.
          joeHeartbeatRestart = false;
          if (logger.isDebugEnabled()) {
            logger
                .debug("run() - HEARTBEAT Timer was not running when STOP command arrived. Restart:" + joeHeartbeatRestart); //$NON-NLS-1$
          }
        }
      } else {
        // This should not happen...
        logger.error("Bad Command Arriving at " +
            BrokerTopicNames.JOE_HEARTBEAT_STATUS + " topic." +
            ApplicationConstants.CR + msgString);
      }
      sb.append("----JoeHeartbeatStatusListener----end");
      if (logger.isInfoEnabled()) {
        logger.info(sb.toString());
      }
      if (logger.isTraceEnabled()) {
        logger.trace("run() - end");
      }
    }
  }

  private class MsgListenerStageResult extends MessageListenerForRCP {
    /**
     * Logger for this class
     */
    private final Logger logger = LogManager.getLogger(Activator.PLUGIN_ID +
        LogSelector.getLogger());

    private String axis;
    private String type;
    private boolean[] readyHome1 = { true, true, true, true, true, true };
    private boolean[] readyHome2 = { false, false, false, false, false, false };

    public void run() {
      if (logger.isTraceEnabled()) {
        logger.trace("run() - start");
      }

      Message msg = message.removeLast();
      String msgString = getMessageString(msg);
      if (msgString == null) {
        return;
      }

      LoisCommand command = null;

      // First we will look at the address to find out which probe
      // the message came from
      OMSCard card = OMSCardUtilities.findCardByAddress(omsCardList,
          msgString.substring(0, msgString.indexOf(' ')));
      String response = msgString.substring(msgString.indexOf(' ') + 1).trim();

      // Check to see if this is a notification flag
      if (msgString.contains("%000 ")) {
        // Message format should be: "localhost:4444 %000 00000001"
        // We will split the message and analyze the last part.
        String[] fields = msgString.split("\\s+");

        if (fields.length == 3) {
          axis = OMSCardUtilities.getAxisAndTypeFromNotificationFlag(fields[2])[0];
          type = OMSCardUtilities.getAxisAndTypeFromNotificationFlag(fields[2])[1];
          String instName = card.getName();
          if (instName.contains("OMS")) {
            instName = "LMI";
          }
          if (OMSCardUtilities.AXIS_NAME_COMMAND_ERROR.equals(axis)) {
            // TODO - Implement Command Error
          } else if (axis != null) {
            if (card.getName().contains("RC")) {
              if (axis.equals("AX")) {
                xStageCommand = true;
              } else if (axis.equals("AY")) {
                yStageCommand = true;
              }
            }

            String key = instName + "." + axisFunctionMap.get(axis);
            command = loisCommandMap.get(key);
            if (command != null) {
              command.getResult().setTimestamp(
                  System.currentTimeMillis() / 1000);
              if (ApplicationConstants.OMSCARD_FLAG_TYPES[0].equals(type)) {
                // Move Completion Notification: DONE
                command.getResult().setCode(0);
                command.getResult().setErrorString("");
              } else if (ApplicationConstants.OMSCARD_FLAG_TYPES[1]
                  .equals(type)) {
                // Move LIMIT Notification
                command.getResult().setCode(1);
                command.getResult().setErrorString("AXIS reached LIMIT.");
                if (axis.equals("AX") || axis.equals("AY")) {
                  stageLimit = true;
                }
              } else if (ApplicationConstants.OMSCARD_FLAG_TYPES[2]
                  .equals(type)) {
                // Move SLIP Notification
                command.getResult().setCode(1);
                command.getResult().setErrorString("Axis has SLIPPED.");
                if (axis.equals("AX") || axis.equals("AY")) {
                  stageSlip = true;
                }
              }

              if (stageLimit) {
                command.getResult().setCode(1);
                command.getResult()
                    .setErrorString("X or Y Axis reached LIMIT.");
              }
              if (stageSlip) {
                command.getResult().setCode(1);
                command.getResult().setErrorString("X or Y Axis has SLIPPED.");
              }

            }
          }

          // TODO - We also have to make sure that the command was a HOME command
          String limitAxis = OMSCardUtilities
              .getAxisNameFromLimitNotificationFlag(fields[2]);
          if (limitAxis != null) {
            ComplexHome: for (OMSCardAxis homeAxis : card.getAxisList()) {
              if (homeAxis.getName().equals(limitAxis)) {
                for (int i = 0; i < ApplicationConstants.COMPLEX_HOME_PROBE_FUNCTIONS.length; i++) {
                  if (ApplicationConstants.COMPLEX_HOME_PROBE_FUNCTIONS[i]
                      .equals(homeAxis.getFunction()) &&
                      fmComplexHomeMap
                          .get(ApplicationConstants.COMPLEX_HOME_PROBE_FUNCTIONS[i])) {
                    if (logger.isDebugEnabled()) {
                      logger
                          .debug("run() - Limit detected for complex home function: " +
                              ApplicationConstants.COMPLEX_HOME_PROBE_FUNCTIONS[i] +
                              " - readyHome1,readyHome2: " +
                              readyHome1[i] +
                              "," + readyHome2[i]);
                    }
                    if (readyHome1[i]) {
                      OMSCardAxis axis = OMSCardUtilities.findAxisByFunction(
                          omsCardList,
                          ApplicationConstants.COMPLEX_HOME_PROBE_FUNCTIONS[i]);
                      GwavesStageCommand stageCommand = new GwavesStageCommand();
                      stageCommand.setAxis(axis);
                      // The HR command completed by hitting the limit switch
                      // and sending out a notification. Let's back off 2mm,
                      // reduce velocity and go HOME again
                      stageCommand.composeCommand(Math.round(2.0f * Math
                          .round(axis.getConversionFactor())), true);
                      StringBuilder sb = new StringBuilder();
                      sb.append(stageCommand.getCommand()).append(" ");
                      // Lowering the velocity to 10% of the standard
                      sb.append("VL")
                          .append(
                              Math.round(stageCommand.getAxis().getVelocity() * 0.1f))
                          .append("; ").append(axis.getHomeCommand());
                      stageCommand.setCommand(sb.toString());
                      try {
                        Messenger.sendDistributedMessage(
                            BrokerTopicNames.JOE_STAGE_QUEUE,
                            stageCommand.toXml(), false);
                        if (logger.isInfoEnabled()) {
                          logger.info(LoggerUtilities
                              .generateBrokerSendLogMessage(
                                  BrokerTopicNames.JOE_STAGE_QUEUE,
                                  stageCommand.toXml()));
                        }
                      } catch (Exception e) {
                        logger.error("run()", e);
                      }
                      readyHome1[i] = false;
                      readyHome2[i] = true;
                    } else if (readyHome2[i]) {
                      OMSCardAxis axis = OMSCardUtilities.findAxisByFunction(
                          omsCardList,
                          ApplicationConstants.COMPLEX_HOME_PROBE_FUNCTIONS[i]);
                      GwavesStageCommand stageCommand = new GwavesStageCommand();
                      stageCommand.setAxis(axis);
                      // The second HR command completed by hitting the limit 
                      // switch and sending out a notification. Let's back off 
                      // 0.2mm and set the position to zero (defining HOME)
                      stageCommand.composeCommand(Math.round(0.2f * Math
                          .round(axis.getConversionFactor())), true);
                      StringBuilder sb = new StringBuilder();
                      sb.append(stageCommand.getCommand()).append(" ");
                      sb.append("LP0; VL")
                          .append(stageCommand.getAxis().getVelocity())
                          .append(";");
                      stageCommand.setCommand(sb.toString());
                      try {
                        Messenger.sendDistributedMessage(
                            BrokerTopicNames.JOE_STAGE_QUEUE,
                            stageCommand.toXml(), false);
                        if (logger.isInfoEnabled()) {
                          logger.info(LoggerUtilities
                              .generateBrokerSendLogMessage(
                                  BrokerTopicNames.JOE_STAGE_QUEUE,
                                  stageCommand.toXml()));
                        }
                      } catch (Exception e) {
                        logger.error("run()", e);
                      }
                      readyHome2[i] = false;
                      readyHome1[i] = true;
                      fmComplexHomeMap.put(
                          ApplicationConstants.COMPLEX_HOME_PROBE_FUNCTIONS[i],
                          Boolean.FALSE);
                    }
                    break ComplexHome;
                  }
                }
              }
            }
          }
          // TODO - Can we implement the code for LMI filter wheels here as well?
          // The check detent command logic for RC1 & RC2 filters
          String functionFilter = instName + ".F";
          String doneAxis = OMSCardUtilities
              .getAxisNameFromDoneNotificationFlag(fields[2]);
          OMSCardAxis filterAxis = OMSCardUtilities.findAxisByFunction(
              omsCardList, functionFilter);
          if ((filterAxis != null) && filterAxis.getName().equals(doneAxis)) {
            // Delay sending the command for 0.5 seconds to give the filter
            // wheel time to settle into the detent.
            try {
              Thread.sleep(500);
            } catch (InterruptedException e) {
              logger.warn("run() - exception ignored", e); //$NON-NLS-1$
            }

            // Let's issue the command now
            sendCheckDetentCommand(filterAxis);
          }
        }
      } else if (msgString
          .contains(ApplicationConstants.OMS_CARD_IDENTIFICATION)) {
        // Result of the WY command - Ignore
      } else if (!msgString.contains("SocketServerSimulator Response")) {
        // We are probably seeing the result of "AA; RP; RQC;" Command or 
        // the result of the "BX" command.
        // BX Example: "10.11.131.92:4321 7c"
        // Example: "10.11.131.92:4321 0,0,200,0,0"
        // We shall identify the axis and apply the data to the telemetry objects
        if (!response.endsWith(ApplicationConstants.RP_END_OF_LINE) &&
            !response.endsWith(ApplicationConstants.RE_END_OF_LINE) &&
            (response.length() != 2)) {
          // We are getting a response to the RQC or RI commands. If it is
          // the response to the RQC command (list of numbers), let's parse
          // the line and find out whether any of the values are under the 
          // threshold. If so, we will send the KILL command.

          char c = response.toCharArray()[0];
          if (Character.isDigit(c)) {
            String pattern = "[\\s,]+";
            String[] params = response.split(pattern);
            if (params.length == 5) {
              for (String param : params) {
                int val = Integer.parseInt(param);
                if (val < ApplicationConstants.RQC_MIN_VALUE) {
                  // Composing the Kill command
                  try {
                    OMSCardAxis axis = card.getAxisList().get(0);
                    GwavesGenericCommand stageCommand = new GwavesGenericCommand();
                    stageCommand.setAxis(axis);
                    stageCommand.setCommand("KL;");
                    Messenger.sendDistributedMessage(
                        BrokerTopicNames.JOE_STAGE_QUEUE, stageCommand.toXml(),
                        false);
                    if (logger.isDebugEnabled()) {
                      logger.debug(LoggerUtilities
                          .generateBrokerSendLogMessage(
                              BrokerTopicNames.JOE_STAGE_QUEUE,
                              stageCommand.toXml()));
                    }
                  } catch (Exception e) {
                    logger.error("run()", e);
                  }
                }
              }
            }
          }
        } else if (response.length() == 2) {
          // We are getting a response to the BX command
          boolean detentState = edu.lowell.lig.common.dct.utils.HelperUtilities
              .decodeFilterDetentState(response);
          int filterNumber = edu.lowell.lig.common.dct.utils.HelperUtilities
              .decodeLMIFilterNumber(response);
          if (card.getName().equals("OMS3")) {
            lmiFilterWheelDetent[0] = detentState;
            lmiFilterWheelPosition[0] = filterNumber;
          } else if (card.getName().equals("OMS4")) {
            lmiFilterWheelDetent[1] = detentState;
            lmiFilterWheelPosition[1] = filterNumber;
          }
        } else if (response.endsWith(ApplicationConstants.RP_END_OF_LINE)) {
          // We are probably seeing the result of "AA; RP;" Command. 
          // Example: "10.11.131.92:4321 0,0,200,0,0"
          String pattern = "[\\s,]+";
          String[] params = response.split(pattern);
          if (params.length == 5) {
            // Getting the position of all the fold mirrors
            int i = 0;
            for (OMSCardAxis axis : card.getAxisList()) {
              if (Arrays.asList(ApplicationConstants.FOLD_MIRROR_FUNCTIONS)
                  .contains(axis.getFunction())) {
                // It is one of the FM axes. If the axis is AX, index=0 and 
                // if the axis is AY, index=1
                if (axis.getName().equals("AX")) {
                  i = 0;
                } else if (axis.getName().equals("AY")) {
                  i = 1;
                } else {
                  // send an error message & continue with the loop
                  List<String> messageList = new ArrayList<String>();
                  messageList
                      .add("We are trying to read an stage RP response for a FM.");
                  messageList.add("OMS card response: " + msgString);
                  messageList.add("index: " + i);
                  messageList.add("axis: " + axis);
                  ErrorMessage em = new ErrorMessage(ErrorLevel.SEVERE, this
                      .getClass().getName(), messageList,
                      System.currentTimeMillis());
                  try {
                    Messenger.sendDistributedMessage(
                        BrokerTopicNames.JOE_ERROR, em.toXml(), false);
                    if (logger.isInfoEnabled()) {
                      logger.info("--> Just sent the JOE Error message:" +
                          ApplicationConstants.CR + em.toString());
                    }
                  } catch (Exception e) {
                    logger.error("run()", e); //$NON-NLS-1$
                  }
                  continue;
                }
                Double val = (Double.parseDouble(params[i]) / axis
                    .getMotorScale());
                fmStageCoordinates.put(axis.getFunction(), val);
              } else if (ApplicationConstants.INSTRUMENT_COVER_FUNCTION
                  .equals(axis.getFunction())) {
                // It is  the IC axis, AZ, index=2
                if (axis.getName().equals("AZ")) {
                  i = 2;
                } else {
                  // send an error message & continue with the loop
                  List<String> messageList = new ArrayList<String>();
                  messageList
                      .add("We are trying to read an stage RP response for the IC.");
                  messageList.add("OMS card response: " + msgString);
                  messageList.add("index: " + i);
                  messageList.add("axis: " + axis);
                  ErrorMessage em = new ErrorMessage(ErrorLevel.SEVERE, this
                      .getClass().getName(), messageList,
                      System.currentTimeMillis());
                  try {
                    Messenger.sendDistributedMessage(
                        BrokerTopicNames.JOE_ERROR, em.toXml(), false);
                    if (logger.isInfoEnabled()) {
                      logger.info("--> Just sent the JOE Error message:" +
                          ApplicationConstants.CR + em.toString());
                    }
                  } catch (Exception e) {
                    logger.error("run()", e); //$NON-NLS-1$
                  }
                  continue;
                }
                icStageCoordinate = Double.parseDouble(params[i]) /
                    axis.getMotorScale();
              }
            }
          }
        }
      }

      if (command != null) {
        boolean sendCommand = false;
        String instName = card.getName();
        if (instName.contains("OMS")) {
          instName = "LMI";
        }
        String key = instName + "." + axisFunctionMap.get(axis);
        if (key.contains("STAGE")) {
          if (xStageCommand && yStageCommand) {
            sendCommand = true;
            xStageCommand = false;
            yStageCommand = false;
            stageLimit = false;
            stageSlip = false;
          }
        } else {
          sendCommand = true;
        }
        if (sendCommand) {
          Messenger.sendDistributedMessage(
              instrumentTopicMap.get(card.getName()), command.getResponse(),
              false);
          if (logger.isInfoEnabled()) {
            logger.info("LOIS Command Response " +
                LoggerUtilities.generateBrokerSendLogMessage(
                    instrumentTopicMap.get(card.getName()),
                    command.getResponse()));
          }
          loisCommandMap.remove(key);
        }
      }

      if (logger.isTraceEnabled()) {
        logger.trace("run() - end");
      }
    }
  }

  // Messages should all be in the following formats:
  // RC1=WFS
  // RC2=GDR
  // HOME=IC
  // HOME=FM-A
  // AUTO_FOCUS_ENABLED=RC1
  // AUTO_FOCUS_DISABLED=RC2
  // rotPA=xxx.x
  private class MsgListenerJoeCommand extends MessageListenerForRCP {
    /**
     * Logger for this class
     */
    private final Logger logger = LogManager.getLogger(Activator.PLUGIN_ID +
        LogSelector.getLogger());

    public void run() {
      if (logger.isTraceEnabled()) {
        logger.trace("run() - start");
      }

      Message msg = message.removeLast();
      String msgString = getMessageString(msg);
      if (msgString == null) {
        return;
      }

      if (logger.isInfoEnabled()) {
        logger.info("--> Message arriving in " + BrokerTopicNames.JOE_COMMAND +
            " topic: " + msgString);
      }

      if (msgString.contains("HOME")) {
        fmComplexHomeMap.put(
            msgString.trim().split(ApplicationConstants.INFO_DELIMITER)[1],
            Boolean.TRUE);
      } else if (msgString.contains("RECONNECT")) {
    	System.out.println("Got RECONNECT message!!!!");
        String cardName = msgString.trim().split(
            ApplicationConstants.INFO_DELIMITER)[1];
        OMSCard card = OMSCardUtilities.findCardByName(omsCardList, cardName);
        // If card is connected, we will terminate the connection & reconnect
        if (card.isConnected()) {
          card.terminateConnection();
          card.makeConnection();
          //  Dyer Lytle
          System.out.println("calling replaceTerminatedThread");
          stageCommandRouter.replaceTerminatedThread(card);
          stageCommandRouter.restartAllOMSCardRouters();
          if (logger.isInfoEnabled()) {
            logger.info("replaced thread for card in stageCommandRouter. - " +
                card.getName());
          }
          //  end Dyer Lytle
        } else {
          card.makeConnection();
          // Dyer Lytle
          stageCommandRouter.restartAllOMSCardRouters();
          stageCommandRouter.replaceTerminatedThread(card);
          if (logger.isInfoEnabled()) {
            logger.info("replaced thread for card in stageCommandRouter. - " +
                card.getName());
          }
          // end Dyer Lytle
        }
      } else if (msgString.contains(ProbeName.RC2.toString())) {
        probeStatus.put(
            ProbeName.RC2,
            ProbeState.valueOf(msgString.trim().split(
                ApplicationConstants.INFO_DELIMITER)[1]));
      } else if (msgString.contains(ProbeName.RC1.toString())) {
        probeStatus.put(
            ProbeName.RC1,
            ProbeState.valueOf(msgString.trim().split(
                ApplicationConstants.INFO_DELIMITER)[1]));
      } else if (msgString
          .startsWith(ApplicationConstants.AUTO_FOCUS_ENABLED_COMMAND)) {
        probeAutoFocusStatus.put(
            ProbeName.valueOf(msgString.trim().split(
                ApplicationConstants.INFO_DELIMITER)[1]), Boolean.TRUE);
      } else if (msgString
          .startsWith(ApplicationConstants.AUTO_FOCUS_DISABLED_COMMAND)) {
        probeAutoFocusStatus.put(
            ProbeName.valueOf(msgString.trim().split(
                ApplicationConstants.INFO_DELIMITER)[1]), Boolean.FALSE);
      } else if (msgString.startsWith(ApplicationConstants.ROTPA_COMMAND)) {
        double rotPA = Double.parseDouble(msgString.trim().split(
            ApplicationConstants.INFO_DELIMITER)[1]);
        if (scienceTarget != null) {
          scienceTarget.getRotation().setTheta(rotPA);
          String stMessage = scienceTarget.getTcsScienceTargetConfigXML();
          Messenger.sendDistributedMessage(BrokerTopicNames.TCS_COMMAND_TOPIC,
              stMessage, false);
          if (logger.isInfoEnabled()) {
            logger.info(LoggerUtilities.generateBrokerSendLogMessage(
                BrokerTopicNames.TCS_COMMAND_TOPIC, stMessage));
          }
        }
      }

      if (logger.isTraceEnabled()) {
        logger.trace("run() - end");
      }
    }
  }

  private class MsgListenerJoeRequest extends MessageListenerForRCP {
    /**
     * Logger for this class
     */
    private final Logger logger = LogManager.getLogger(Activator.PLUGIN_ID +
        LogSelector.getLogger());

    public void run() {
      if (logger.isTraceEnabled()) {
        logger.trace("run() - start");
      }

      Message msg = message.removeLast();
      String msgString = getMessageString(msg);
      if (msgString == null) {
        return;
      }
      
      // 6 lines added by D. M. Lytle so JOE won't respond to RC2/RC1 requests
      // until RC2/RC1 is fixed.
//      if(msgString.contains("RC1")) {
//    	  return;
//      }
//      if(msgString.contains("RC2")) {
//    	  return;
//      }

      if (logger.isDebugEnabled()) {
        logger.debug("--> Message arriving in " + BrokerTopicNames.JOE_REQUEST +
            " topic: " + msgString);
      }

      // Message string is in the format of "query-type=query-parameter". e.g.:
      // "probeCoordinate.Query=GDR"
      // "probeName.Query=WFS"
      // "probeState.Query=RC2"
      // "Query=tcsImageRotationAngle"
      // "Query=tcsImageParallacticAngle"
      // "Query=stageReadoutStatus"
      // "Query=probeFunctionStatus"
      // We shall parse it and proceed from there.
      try {
        String queryType = msgString.trim().split(
            ApplicationConstants.INFO_DELIMITER)[0].trim();
        String queryParam = msgString.trim().split(
            ApplicationConstants.INFO_DELIMITER)[1].trim();
        StringBuilder sb = new StringBuilder();

        if (queryType.equals(ApplicationConstants.PROBE_STATE_QUERY)) {
          sb.append(edu.lowell.lig.common.utils.HelperUtilities
              .generateNameValueInfo(queryParam,
                  probeStatus.get(ProbeName.valueOf(queryParam)).toString()));
          Messenger.sendDistributedMessage(BrokerTopicNames.JOE_REPLY,
              sb.toString(), msg.getJMSMessageID(), false);
        } else if (queryType.equals(ApplicationConstants.PROBE_NAME_QUERY)) {
          sb.append(edu.lowell.lig.common.utils.HelperUtilities
              .generateNameValueInfo(
                  getProbeName(ProbeState.valueOf(queryParam)), queryParam));
          Messenger.sendDistributedMessage(BrokerTopicNames.JOE_REPLY,
              sb.toString(), msg.getJMSMessageID(), false);
        } else if (queryType
            .equals(ApplicationConstants.PROBE_FOCAL_PLANE_COORDINATE_QUERY)) {
          sb.append(HelperUtilities.generateProbeCoordinateInfo(queryParam,
              probeFocalPlaneCoordinates.get(ProbeState.valueOf(queryParam))));
          Messenger.sendDistributedMessage(BrokerTopicNames.JOE_REPLY,
              sb.toString(), msg.getJMSMessageID(), false);
        } else if (queryType
            .equals(ApplicationConstants.PROBE_STAGE_COORDINATE_QUERY)) {
          sb.append(HelperUtilities.generateProbeCoordinateInfo(queryParam,
              probeStageCoordinates.get(ProbeName.valueOf(queryParam))));
          Messenger.sendDistributedMessage(BrokerTopicNames.JOE_REPLY,
              sb.toString(), msg.getJMSMessageID(), false);
        } else if (queryType.equals(ApplicationConstants.QUERY)) {
          if (queryParam
              .equals(ApplicationConstants.TCS_ROTATOR_POSITION_ANGLE)) {
            sb.append(tcsData
                .get(ApplicationConstants.TCS_ROTATOR_POSITION_ANGLE));
            Messenger.sendDistributedMessage(BrokerTopicNames.JOE_REPLY,
                sb.toString(), msg.getJMSMessageID(), false);
          } else if (queryParam
              .equals(ApplicationConstants.TCS_INSTRUMENT_ALIGNMENT_ANGLE)) {
            sb.append(tcsData
                .get(ApplicationConstants.TCS_INSTRUMENT_ALIGNMENT_ANGLE));
            Messenger.sendDistributedMessage(BrokerTopicNames.JOE_REPLY,
                sb.toString(), msg.getJMSMessageID(), false);
          } else if (queryParam
              .equals(ApplicationConstants.TCS_ROTATOR_INSTRUMENT_POSITION_ANGLE)) {
            sb.append(tcsData
                .get(ApplicationConstants.TCS_ROTATOR_INSTRUMENT_POSITION_ANGLE));
            Messenger.sendDistributedMessage(BrokerTopicNames.JOE_REPLY,
                sb.toString(), msg.getJMSMessageID(), false);
          } else if (queryParam
              .equals(ApplicationConstants.TCS_IMAGE_PARALLACTIC_ANGLE)) {
            sb.append(tcsData
                .get(ApplicationConstants.TCS_IMAGE_PARALLACTIC_ANGLE));
            Messenger.sendDistributedMessage(BrokerTopicNames.JOE_REPLY,
                sb.toString(), msg.getJMSMessageID(), false);
          } else if (queryParam.equals(ApplicationConstants.MOUNT_GUIDE_MODE)) {
            sb.append(tcsData.get(ApplicationConstants.MOUNT_GUIDE_MODE));
            Messenger.sendDistributedMessage(BrokerTopicNames.JOE_REPLY,
                sb.toString(), msg.getJMSMessageID(), false);
          } else if (queryParam.equals(ApplicationConstants.SCIENCE_TARGET)) {
            sb.append(tcsData.get(ApplicationConstants.SCIENCE_TARGET));
            Messenger.sendDistributedMessage(BrokerTopicNames.JOE_REPLY,
                sb.toString(), msg.getJMSMessageID(), false);
          } else if (queryParam.equals(ApplicationConstants.FOLD_MIRRORS_STATE)) {
            // we need to generate a list of all 4 states
            StringBuilder tmp = new StringBuilder();
            for (String s : ApplicationConstants.FOLD_MIRROR_FUNCTIONS) {
              tmp.append(fmStateMap.get(s)).append(",");
            }
            sb.append(edu.lowell.lig.common.utils.HelperUtilities
                .generateNameValueInfo(queryParam,
                    tmp.substring(0, tmp.length() - 1)));
            Messenger.sendDistributedMessage(
                BrokerTopicNames.JOE_REPLY_BROADCAST, sb.toString(),
                msg.getJMSMessageID(), false);
          } else if (queryParam
              .equals(ApplicationConstants.INSTRUMENT_COVER_STATE)) {
            sb.append(edu.lowell.lig.common.utils.HelperUtilities
                .generateNameValueInfo(queryParam, icState.toString()));
            Messenger.sendDistributedMessage(
                BrokerTopicNames.JOE_REPLY_BROADCAST, sb.toString(),
                msg.getJMSMessageID(), false);
          } else if (queryParam
              .equals(ApplicationConstants.PROBE_FUNCTION_STATUS)) {
        	// D.M.Lytle Comment out 9 lines below if RC1 broken.
            String strMessage = edu.lowell.lig.common.utils.HelperUtilities
                .generateNameValueInfo(ProbeName.RC1.toString(), probeStatus
                    .get(ProbeName.RC1).toString());
            Messenger.sendDistributedMessage(BrokerTopicNames.JOE_COMMAND,
                strMessage, false);
            if (logger.isInfoEnabled()) {
              logger.info(LoggerUtilities.generateBrokerSendLogMessage(
                  BrokerTopicNames.JOE_COMMAND, strMessage));
            }
//        	  System.out.println("RC1 and RC2 Disabled.");
            // 9 lines comment out by D.M.Lytle if RC2 broken.
            strMessage = edu.lowell.lig.common.utils.HelperUtilities
                .generateNameValueInfo(ProbeName.RC2.toString(), probeStatus
                    .get(ProbeName.RC2).toString());
            Messenger.sendDistributedMessage(BrokerTopicNames.JOE_COMMAND,
                strMessage, false);
            if (logger.isInfoEnabled()) {
              logger.info(LoggerUtilities.generateBrokerSendLogMessage(
                  BrokerTopicNames.JOE_COMMAND, strMessage));
            }
          }
        }
        if (logger.isDebugEnabled()) {
          logger.debug(LoggerUtilities.generateBrokerSendLogMessage(
              BrokerTopicNames.JOE_REPLY, sb.toString()));
        }
      } catch (JMSException e) {
        logger.error("run()", e);
      }

      if (logger.isTraceEnabled()) {
        logger.trace("run() - end");
      }
    }
  }

  private class MsgListenerScienceTarget extends MessageListenerForRCP {
    /**
     * Logger for this class
     */
    private final Logger logger = LogManager.getLogger(Activator.PLUGIN_ID +
        LogSelector.getLogger());

    public void run() {
      if (logger.isTraceEnabled()) {
        logger.trace("run() - start");
      }

      Message msg = message.removeLast();
      String msgString = getMessageString(msg);
      if (msgString == null) {
        return;
      }

      if (logger.isInfoEnabled()) {
        logger.info(LoggerUtilities.generateBrokerReceiveLogMessage(
            getTopic(msg), msgString));
      }

      try {
        // Deserializing
        Serializer serializer = new Persister(new Format(2));
        NewScienceTarget tcsNewScienceTarget = serializer.read(
            NewScienceTarget.class, msgString);

        // We want to see if the new science target is the result of an offset
        // command so we will check the following:
        // - the last offset timestamp is within 1 second
        // - Check to see if scienceTargetConfiguration in the new scienceTarget
        //   has changed. If it has not, the new scienceTarget may be the result
        //   of an offset command.
        if (((msg.getJMSTimestamp() - offsetTimestamp) < 1000) &&
            tcsNewScienceTarget.getScienceTargetConfiguration().equals(
                scienceTargetConfiguration) && !slewCommand) {
          // Let's compare offset to the dither tolerance for the current guide star.
          // TODO - We should not perform this check if it is on-chip guiding
          if (!guideOnChip) {
            double ditherTolerance = Double.parseDouble(tcsData
                .get(ApplicationConstants.DITHER_TOLERANCE));
            if (offset <= ditherTolerance) {
              offsetCommand = true;
            } else {
              // TODO - Need to cancel the operation
              offsetCommand = false;
            }
            if (logger.isInfoEnabled()) {
              logger.info("==> Same ScienceTargetConfiguration - offset=" +
                  offset);
              logger.info("==> offsetCommand=" + offsetCommand +
                  " - ditherTolerance=" + ditherTolerance);
            }
          }
        } else {
          if (logger.isInfoEnabled()) {
            logger.info("==> Different ScienceTargetConfiguration" +
                ApplicationConstants.CR +
                "Old ScienceTargetConfiguration=" +
                ObjectUtils.toString(scienceTargetConfiguration) +
                " - New ScienceTargetConfiguration=" +
                ObjectUtils.toString(tcsNewScienceTarget
                    .getScienceTargetConfiguration()));
          }
          scienceTargetConfiguration = tcsNewScienceTarget
              .getScienceTargetConfiguration();
          offsetCommand = false;
          slewCommand = false;
        }
        scienceTarget = new ScienceTarget(tcsNewScienceTarget);
        tcsData.put(ApplicationConstants.SCIENCE_TARGET, scienceTarget.toXml());
      } catch (Exception e) {
        logger.error("run()", e);
      }

      // Send Guide Off command if we are not doing an offset
      if (!offsetCommand) {
        edu.lowell.lig.common.dct.utils.HelperUtilities
            .sendGuiderCommand(GuideStateTypes.Off);
        if (logger.isInfoEnabled()) {
          logger
              .info("==> This is not a guided offset so we should turn guide off on TCS.");
        }
      } else if (tcsData.get(ApplicationConstants.MOUNT_GUIDE_MODE).equals(
          MountGuideModeTypes.ClosedLoop.toString())) {
        // We have had an offset command and the TCS is in closed loop mode.
        // Let's send a message to the GDR to get the new probe coordinates 
        // and move the probe.
        Messenger.sendDistributedMessage(BrokerTopicNames.JOE_GUIDER_ACTIONS,
            ApplicationConstants.GUIDED_OFFSET_ENABLED, false);
        if (logger.isInfoEnabled()) {
          logger
              .info("==> Offset command within dither limit in ClosedLoop mode.");
          logger.info(LoggerUtilities.generateBrokerSendLogMessage(
              BrokerTopicNames.JOE_GUIDER_ACTIONS,
              ApplicationConstants.GUIDED_OFFSET_ENABLED));
        }
        offsetCommand = false;
        // TODO - How do we get back? absorb or another offset?
      } else if (!tcsData.get(ApplicationConstants.MOUNT_GUIDE_MODE).equals(
          MountGuideModeTypes.ClosedLoop.toString())) {
        // We have had an offset command and the TCS is NOT in closed loop mode.
        // Let's set offsetCommand  to false and there is nothing else to do.
        if (logger.isInfoEnabled()) {
          logger
              .info("==> Offset command within dither limit but NOT in ClosedLoop mode.");
        }
        offsetCommand = false;
      }

      if (logger.isTraceEnabled()) {
        logger.trace("run() - end");
      }
    }
  }

  /**
   * This listener listens for the TCS command which turns Guide On. When it
   * sees the command, copies the track ID from the current science target to be
   * used by the GuiderDeltasMessageListener class.
   * 
   * @author szoonem
   */
  private class MsgListenerTcsCommand extends MessageListenerForRCP {
    /**
     * Logger for this class
     */
    private final Logger logger = LogManager.getLogger(Activator.PLUGIN_ID +
        LogSelector.getLogger());

    public void run() {
      if (logger.isTraceEnabled()) {
        logger.trace("run() - start");
      }

      Message msg = message.removeLast();
      String msgString = getMessageString(msg);
      if (msgString == null) {
        return;
      }

      if (logger.isInfoEnabled()) {
        logger.info(LoggerUtilities.generateBrokerReceiveLogMessage(
            getTopic(msg), msgString));
      }

      if (msgString.startsWith("<guide>")) {
        try {
          // Deserializing
          Serializer serializer = new Persister(new Format(2));
          Guide gd = serializer.read(Guide.class, msgString);
          if (gd.getGuideState() == GuideStateTypes.On) {
            scienceTrackId = scienceTarget.getTrackId();
          }
        } catch (Exception e) {
          logger.error("run()", e);
        }
      } else if (msgString.startsWith("<scienceTargetOffset>")) {

        try {
          // Deserializing
          Serializer serializer = new Persister(new Format(2));
          ScienceTargetOffset sto = serializer.read(ScienceTargetOffset.class,
              msgString);
          offsetTimestamp = msg.getJMSTimestamp();
          // Calculating the offset in arc-seconds
          if ((sto.getOffsetDef().getOffsetType() == OffsetTypeValues.TPLANE) ||
              (sto.getOffsetDef().getOffsetType() == OffsetTypeValues.SIMPLE)) {
            double off1 = sto.getOffsetDef().getOff1() * 15.0;
            double off2 = sto.getOffsetDef().getOff2();
            offset = Math.sqrt(off1 * off1 + off2 * off2);
          } else {
            // offset type is PA
            offset = sto.getOffsetDef().getOff1() * 15.0;
          }
        } catch (Exception e) {
          logger.error("run()", e);
        }
      } else if (msgString.startsWith("<scienceTargetClearOffset>")) {
        // Clear Offsets command has been issued which is equivalent to 0,0
        try {
          offsetTimestamp = msg.getJMSTimestamp();
          offset = 0.0;
        } catch (Exception e) {
          logger.error("run()", e);
        }
      }

      if (logger.isTraceEnabled()) {
        logger.trace("run() - end");
      }
    }
  }

  private class MsgListenerAosSettled extends MessageListenerForRCP {
    /**
     * Logger for this class
     */
    private final Logger logger = LogManager.getLogger(Activator.PLUGIN_ID +
        LogSelector.getLogger());

    public void run() {
      if (logger.isTraceEnabled()) {
        logger.trace("run() - start");
      }

      Message msg = message.removeLast();
      String msgString = getMessageString(msg);
      if (msgString == null) {
        return;
      }

      LoisCommand command = null;
      boolean aosSettled = Boolean.parseBoolean(msgString);
      if (logger.isInfoEnabled()) {
        logger.info(LoggerUtilities.generateBrokerReceiveLogMessage(
            getTopic(msg), msgString));
      }

      if (aosSettled) {
        String key = "LMI.FOCUS";
        command = loisCommandMap.get(key);
        if (command != null) {
          // AOS has settled
          command.getResult().setTimestamp(System.currentTimeMillis() / 1000);
          command.getResult().setCode(0);
          command.getResult().setErrorString("");

          Messenger.sendDistributedMessage(
              BrokerTopicNames.JOE_TCS_COMMAND_RESULT, command.getResponse(),
              false);
          if (logger.isInfoEnabled()) {
            logger.info("LOIS Command Response " +
                LoggerUtilities.generateBrokerSendLogMessage(
                    BrokerTopicNames.JOE_TCS_COMMAND_RESULT,
                    command.getResponse()));
          }
          loisCommandMap.remove(key);
        }
      }

      if (logger.isTraceEnabled()) {
        logger.trace("run() - end");
      }
    }
  }

  private class MsgListenerAosRelativeFocusOffset extends MessageListenerForRCP {
    /**
     * Logger for this class
     */
    private final Logger logger = LogManager.getLogger(Activator.PLUGIN_ID +
        LogSelector.getLogger());

    public void run() {
      if (logger.isTraceEnabled()) {
        logger.trace("run() - start");
      }

      Message msg = message.removeLast();
      String msgString = getMessageString(msg);
      if (msgString == null) {
        return;
      }

      // AOS relative move is in meters and should be converted to microns.
      float aosMove = Float.parseFloat(msgString) * 1.0E+6f;

      // send the command to RC1 and RC2
      // D.M.Lytle comment next 5 lines while RC1 broken.
      if (probeAutoFocusStatus.get(ProbeName.RC1)) {
        HelperUtilities.sendProbeStageMove(
            probeAutoFocusScaleFactor.get(ProbeName.RC1) * aosMove, "RC1.Z",
            true);
      }
      // D.M.Lytle - comment the next 5 lines out while RC2 is broken.
      if (probeAutoFocusStatus.get(ProbeName.RC2)) {
        HelperUtilities.sendProbeStageMove(
            probeAutoFocusScaleFactor.get(ProbeName.RC2) * aosMove, "RC2.Z",
            true);
      }

      //aosFocusOffset += aosMove;

      if (logger.isTraceEnabled()) {
        logger.trace("run() - end");
      }
    }
  }

  private class MsgListenerTcsInPosition extends MessageListenerForRCP {
    /**
     * Logger for this class
     */
    private final Logger logger = LogManager.getLogger(Activator.PLUGIN_ID +
        LogSelector.getLogger());

    public void run() {
      if (logger.isTraceEnabled()) {
        logger.trace("run() - start");
      }

      Message msg = message.removeLast();
      LoisCommand command = null;
      boolean tcsInPosition = Boolean.parseBoolean(getMessageString(msg));

      if (tcsInPosition) {
        String key = "LMI.OFFSET";
        command = loisCommandMap.get(key);
        if (command != null) {
          // TCS is in position
          command.getResult().setTimestamp(System.currentTimeMillis() / 1000);
          command.getResult().setCode(0);
          command.getResult().setErrorString("");

          Messenger.sendDistributedMessage(
              BrokerTopicNames.JOE_TCS_COMMAND_RESULT, command.getResponse(),
              false);
          if (logger.isInfoEnabled()) {
            logger.info("LOIS Command Response " +
                LoggerUtilities.generateBrokerSendLogMessage(
                    BrokerTopicNames.JOE_TCS_COMMAND_RESULT,
                    command.getResponse()));
          }
          loisCommandMap.remove(key);
        }
      }

      // Let's update the tcsActivity file
      Writer writer = null;
      try {
        writer = new BufferedWriter(new OutputStreamWriter(
            new FileOutputStream("/var/log/LOUI/tcsActivity.txt"), "utf-8"));
        writer.write(tcsData.get(ApplicationConstants.TCS_CURRENT_RA) + " " +
            tcsData.get(ApplicationConstants.TCS_CURRENT_DEC) + " " +
            tcsInPosition);
      } catch (IOException ex) {
        logger.warn("run() - exception ignored", ex); //$NON-NLS-1$
      } finally {
        try {
          writer.close();
        } catch (Exception ex) {
          logger.warn("run() - exception ignored", ex); //$NON-NLS-1$
        }
      }

      if (logger.isTraceEnabled()) {
        logger.trace("run() - end");
      }
    }
  }

  private class MsgListenerWFSAccumuator extends MessageListenerForRCP {
    /**
     * Logger for this class
     */
    private final Logger logger = LogManager.getLogger(Activator.PLUGIN_ID +
        LogSelector.getLogger());

    public void run() {
      if (logger.isTraceEnabled()) {
        logger.trace("run() - start");
      }

      Message msg = message.removeLast();
      String msgString = getMessageString(msg);
      if (msgString == null) {
        return;
      }

      // First we get the command.
      int ind = msgString.indexOf('|');
      WfsAccumulatorCommand command = WfsAccumulatorCommand.valueOf(msgString
          .substring(0, ind));
      String txtData = msgString.substring(ind + 1);

      if (command == WfsAccumulatorCommand.GAD) {
        // Getting the accumulator, putting it on the topic and breaking out
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 47; i++) {
          sb.append(wfsAccumulator[i]);
          sb.append(" ");
        }
        sb.append(wfsAccumulator[47]);
        Messenger.sendDistributedMessage(
            BrokerTopicNames.JOE_WFS_ACCUMULATED_DATA_TOPIC, sb.toString(),
            false);
        if (logger.isInfoEnabled()) {
          logger.info(LoggerUtilities.generateBrokerSendLogMessage(
              BrokerTopicNames.JOE_WFS_ACCUMULATED_DATA_TOPIC, sb.toString()));
        }
      } else if (command == WfsAccumulatorCommand.CJA) {
        // Clearing the accumulator and breaking out
        wfsAccumulator = new double[48];
        if (logger.isInfoEnabled()) {
          logger.info("Receiving CJA Command - double[] wfsAccumulator=" +
              Arrays.toString(wfsAccumulator));
        }
      } else {
        if (command == WfsAccumulatorCommand.CAZ) {
          // Clearing the accumulator
          wfsAccumulator = new double[48];

        } else if (command == WfsAccumulatorCommand.SAD) {
          // Accumulating the data
          String pattern = "[\\s,]+";
          String[] params = txtData.split(pattern);
          if (params.length != 48) {
            logger
                .error("run() - Incorrect number of parameters for WavefrontData object"); //$NON-NLS-1$
          } else {
            for (int i = 0; i < 47; i++) {
              wfsAccumulator[i] += Double.parseDouble(params[i]);
            }
            wfsAccumulator[47] = Double.parseDouble(params[47]);
          }
        }
        // Falling through the two commands that send the message to AOS
        String wfdXML = HelperUtilities.getWavefrontDataAsXML(wfsAccumulator);
        Messenger.sendDistributedMessage(BrokerTopicNames.AOS_WFS_DATA_TOPIC,
            wfdXML, false);
        if (logger.isInfoEnabled()) {
          logger.info(LoggerUtilities.generateBrokerSendLogMessage(
              BrokerTopicNames.AOS_WFS_DATA_TOPIC, wfdXML));
        }
      }

      if (logger.isTraceEnabled()) {
        logger.trace("run() - end");
      }
    }
  }

  private class MsgListenerGuiderActions extends MessageListenerForRCP {
    /**
     * Logger for this class
     */
    private final Logger logger = LogManager.getLogger(Activator.PLUGIN_ID +
        LogSelector.getLogger());

    public void run() {
      if (logger.isTraceEnabled()) {
        logger.trace("run() - start"); //$NON-NLS-1$
      }

      Message msg = message.removeLast();
      String msgString = getMessageString(msg);
      if (msgString == null) {
        return;
      }

      if (logger.isInfoEnabled()) {
        logger.info("run() - " + msgString);
      }

      String[] params = msgString.trim().split(
          ApplicationConstants.INFO_DELIMITER);
      if (params[0].trim().equals(ApplicationConstants.DITHER_TOLERANCE)) {
        tcsData.put(ApplicationConstants.DITHER_TOLERANCE, params[1].trim());
      } else if (params[0].trim().equals(ApplicationConstants.SLEW_COMMAND)) {
        slewCommand = true;
      } else if (msgString.startsWith(ApplicationConstants.GUIDE_ON_CHIP)) {
        guideOnChip = Boolean.parseBoolean(params[1].trim());
        if (logger.isInfoEnabled()) {
          logger.info("--> guideOnChip=" + guideOnChip);
        }
      }

      if (logger.isTraceEnabled()) {
        logger.trace("run() - end"); //$NON-NLS-1$
      }
    }
  }
}
