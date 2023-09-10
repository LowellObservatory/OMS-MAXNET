package edu.lowell.lig.joe.listener;

import java.util.ArrayList;
import java.util.List;

import javax.jms.Message;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.simpleframework.xml.Serializer;
import org.simpleframework.xml.core.Persister;
import org.simpleframework.xml.stream.Format;

import edu.lowell.lig.common.dct.model.FoldMirrorState;
import edu.lowell.lig.common.dct.model.GwavesGenericCommand;
import edu.lowell.lig.common.dct.model.GwavesStageCommand;
import edu.lowell.lig.common.dct.model.InstrumentCoverState;
import edu.lowell.lig.common.dct.model.OMSCard;
import edu.lowell.lig.common.dct.utils.HelperUtilities;
import edu.lowell.lig.common.dct.utils.OMSCardUtilities;
import edu.lowell.lig.common.model.ErrorLevel;
import edu.lowell.lig.common.model.ErrorMessage;
import edu.lowell.lig.common.utils.ApplicationConstants;
import edu.lowell.lig.common.utils.DateTimeMatcher;
import edu.lowell.lig.jms.core.MessageListenerForRCP;
import edu.lowell.lig.jms.core.Messenger;
import edu.lowell.lig.jms.utils.BrokerTopicNames;
import edu.lowell.lig.joe.Activator;
import edu.lowell.lig.joe.SocketTester;   // Lytle
import edu.lowell.lig.joe.Application;
import edu.lowell.lig.joe.OMSSocketRouter;
import edu.lowell.loui.logging.LogSelector;

public class StageCommandRouter extends MessageListenerForRCP {
  /**
   * Logger for this class
   */
  private final Logger logger = LogManager.getLogger(Activator.PLUGIN_ID +
      LogSelector.getLogger());
  private static final Logger socketReadoutLogger = LogManager
      .getLogger(Activator.PLUGIN_ID + ".socketReadout" +
          LogSelector.getLogger());

  private List<OMSCard> omsCardList = new ArrayList<OMSCard>();
  private List<OMSSocketRouter> omsCardRouterList = new ArrayList<OMSSocketRouter>();

  private GwavesStageCommand stageCommand;

  /**
   * 
   */
  public StageCommandRouter() {
    super();
    this.omsCardList = HelperUtilities.getOmsCardList();
    for (OMSCard card : this.omsCardList) {
      card.makeConnection();
      OMSSocketRouter socketRouter = new OMSSocketRouter(card);
      this.omsCardRouterList.add(socketRouter);
      if (logger.isInfoEnabled()) {
        logger.info("Adding OMS Card socket router to the list. - " +
            socketRouter.getCard().getName());
      }
    }
    for (int i = 0; i < this.omsCardRouterList.size(); i++) {
      if (this.omsCardRouterList.get(i).getCard().isConnected()) {
        this.omsCardRouterList.get(i).start();
      }
    }
  }

  /**
   * @return the omsCardList
   */
  public List<OMSCard> getOmsCardList() {
    return omsCardList;
  }
  
  // Dyer Lytle added the following routine on Sep. 3, 2023.
  public void restartAllOMSCardRouters() {
	 OMSSocketRouter socketRouter;
	 SocketTester socketTester;
    // Kill all the OMSSocketRouters and SocketTesters.
    for (int i = 0; i < this.omsCardRouterList.size(); i++) {
	  // kill the OMSSocketRouter thread and its SocketTester thread.
      
      socketRouter = this.omsCardRouterList.get(i);
      socketTester = socketRouter.getSocketTester();
      
      System.out.println("terminating connection for " + socketRouter.getName());
      socketRouter.getCard().terminateConnection();
      System.out.println("killing " + socketRouter.getName());
      socketRouter.interrupt();
      
	  try {
	    socketRouter.join();
        System.out.println("socketRouter finished");  
      } catch (InterruptedException e) {
       // Handling the exception
       System.out.println("Interrupted Exception");
      }
      
      System.out.println("terminating connection for " + socketTester.getName());
      socketTester.getDev().terminateConnection();
      System.out.println("killing " + socketTester.getName());
	  socketTester.stopit();
      
	  socketTester = null;
	  socketRouter = null;
      
    }
    omsCardRouterList = null;
    
    
    // Make a new omsCardRouterList
    omsCardRouterList = new ArrayList<OMSSocketRouter>();
    // Recreate the OMS card router list.
    for (OMSCard card : this.omsCardList) {
      card.makeConnection();
      socketRouter = new OMSSocketRouter(card);
      this.omsCardRouterList.add(socketRouter);
      if (logger.isInfoEnabled()) {
        logger.info("Adding OMS Card socket router to the list. - " +
            socketRouter.getCard().getName());
      }
    }
    for (int i = 0; i < this.omsCardRouterList.size(); i++) {
      if (this.omsCardRouterList.get(i).getCard().isConnected()) {
        this.omsCardRouterList.get(i).start();
      }
    }
  }
  
  // Dyer Lytle added the following routine on Aug. 23, 2023.
  /**
   * Replace terminated thread for OMSCard card.
   * Thread was terminated by socket restart.
   */
  public void replaceTerminatedThread(OMSCard card) {
    logger.info("restarting terminated threads");
    for (int i = 0; i < this.omsCardRouterList.size(); i++) {
    	System.out.println(this.omsCardRouterList.get(i).isAlive());
      if (!this.omsCardRouterList.get(i).isAlive()) {
    	logger.info("found terminated thread, replacing");
    	System.out.println("about to interrupt the thread");
    	// kill the OMSSocketRouter thread and its SocketTester thread.
    	this.omsCardRouterList.get(i).getSocketTester().interrupt(); // update Lytle 091223
    	this.omsCardRouterList.get(i).interrupt();  // update Lytle 091223
    	// Remove the OMSCardRouter thread from the list
		omsCardRouterList.remove(this.omsCardRouterList.get(i));
		// Start a new OMSSocketRouter
		OMSSocketRouter socketRouter = new OMSSocketRouter(card);
		this.omsCardRouterList.add(socketRouter);
	  }
    }
    for (int i = 0; i < this.omsCardRouterList.size(); i++) {
    	if (this.omsCardRouterList.get(i).getState().toString() == "NEW") {
    	  logger.info("found new thread, starting");
          this.omsCardRouterList.get(i).start();
      }
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

    Serializer serializer = new Persister(new DateTimeMatcher(), new Format(2));
    try {
      stageCommand = serializer.read(GwavesGenericCommand.class, msgString);

    } catch (Exception e) {
      logger.error("run()", e);
    }

    OMSCard card = OMSCardUtilities.findCardByFunction(this.omsCardList,
        stageCommand.getAxis().getFunction());

    // Check to see if connection has been made
    if (!card.isConnected()) {
      card.makeConnection();
      // Check to see whether the connection is successful and if so,
      // we'll start the read loop.
      if (card.isConnected()) {
        for (int i = 0; i < this.omsCardRouterList.size(); i++) {
          if (this.omsCardRouterList.get(i).getCard().getFullAddress()
              .equals(card.getFullAddress())) {
            this.omsCardRouterList.get(i).start();
            break;
          }
        }
      }
    }

    if (card.isConnected()) {
      if (logger.isDebugEnabled()) {
        socketReadoutLogger.debug("Incoming Stage Command Message:" +
            ApplicationConstants.CR + msgString);
      }
      if (logger.isInfoEnabled()) {
        if (!stageCommand.getCommand().equals(
            ApplicationConstants.ALL_AXES_REPORT_POSITION) &&
            !stageCommand.getCommand().equals(
                ApplicationConstants.ALL_AXES_REPORT_ENCODER)) {
          logger.info("Incoming Stage Command Message:" +
              ApplicationConstants.CR + msgString);
          logger.info("Generated outgoing Stage Command: (" +
              card.getFullAddress() + ") " + stageCommand.getCommand());
        }
      }

      // Setting the state for instrument cover if the command was to 
      // close or open the instrument cover:
      // Open/Retract = HR
      // Close/Extend = HM
      if (stageCommand.getAxis().getFunction().equals("IC")) {
        if (stageCommand.getCommand().contains("HR")) {
          Application.icState = InstrumentCoverState.OPEN;
        } else if (stageCommand.getCommand().contains("HM")) {
          Application.icState = InstrumentCoverState.CLOSED;
        } else if (!stageCommand.getCommand().equals(
            OMSCardUtilities.findAxisByFunction(omsCardList, "IC")
                .getInitCommand()) &&
            !stageCommand.getCommand().contains("LP0")) {
          Application.icState = InstrumentCoverState.UNKNOWN;
        }
      } else if (stageCommand.getAxis().getFunction().contains("FM")) {
        // Setting the state for the fold mirrors if the command was to 
        // extend or retract one of the fold mirrors:
        // Out/Retract = HR
        // In/Extend = HM
        if (stageCommand.getCommand().contains("HR")) {
          // TODO - Ideally we should wait for the limit notification but for 
          // now we will base the logic just on the command.
          Application.fmStateMap.put(stageCommand.getAxis().getFunction(),
              FoldMirrorState.HOME);
        } else if (stageCommand.getCommand().contains("HM") ||
            stageCommand.getCommand().contains("MA")) {
          // This is an Move or Extend command and we will check and see where
          // all of the fold mirrors are. All should be at home/retracted
          // position (except the one being commanded) and if any are not,
          // we will abort the command and send an error message.
          for (String fm : Application.fmStateMap.keySet()) {
            if (!fm.equals(stageCommand.getAxis().getFunction()) &&
                (Application.fmStateMap.get(fm) != FoldMirrorState.HOME)) {
              List<String> messageList = new ArrayList<String>();
              messageList.add("Fold Mirror cannot be extended.");
              messageList.add(fm + " is not at the HOME position.");
              ErrorMessage em = new ErrorMessage(ErrorLevel.SEVERE, this
                  .getClass().getName(), messageList,
                  System.currentTimeMillis());
              try {
                Messenger.sendDistributedMessage(BrokerTopicNames.JOE_ERROR,
                    em.toXml(), false);
                if (logger.isInfoEnabled()) {
                  logger.info("--> Just sent the JOE Error message:" +
                      ApplicationConstants.CR + em.toString());
                }
              } catch (Exception e) {
                logger.error("run()", e); //$NON-NLS-1$
              }

              String rejectedCommand = ApplicationConstants.REJECTED_COMMAND_TEXT +
                  " (" +
                  stageCommand.getAxis().getFunction() +
                  ") " +
                  stageCommand.getCommand();
              Messenger.sendDistributedMessage(
                  BrokerTopicNames.STAGE_COMMAND_REJECTION, rejectedCommand,
                  false);
              if (logger.isInfoEnabled()) {
                logger.info("--> " + rejectedCommand);
              }
              return;
            }
          }

          // This is a Move or Extend command and we have already checked
          // the command state of all the other cards. We will now check the
          // position (motor step read-back) of the other stages. All should 
          // be at less than 2.0mm (except the one being commanded) and if 
          // any are not, we will abort the command and send an error message.
          for (String fm : Application.fmStateMap.keySet()) {
            if (!fm.equals(stageCommand.getAxis().getFunction()) &&
                !((Application.fmStageCoordinates.get(fm) < 2.0) && (Application.fmStageCoordinates
                    .get(fm) > -0.2))) {
              List<String> messageList = new ArrayList<String>();
              messageList.add("Fold Mirror cannot be extended.");
              messageList.add(fm + " is not retracted enough.");
              messageList.add("Current position: " +
                  ApplicationConstants.angleFormatter
                      .format(Application.fmStageCoordinates.get(fm)));
              ErrorMessage em = new ErrorMessage(ErrorLevel.SEVERE, this
                  .getClass().getName(), messageList,
                  System.currentTimeMillis());
              try {
                Messenger.sendDistributedMessage(BrokerTopicNames.JOE_ERROR,
                    em.toXml(), false);
                if (logger.isInfoEnabled()) {
                  logger.info("--> Just sent the JOE Error message:" +
                      ApplicationConstants.CR + em.toString());
                }
              } catch (Exception e) {
                logger.error("run()", e); //$NON-NLS-1$
              }

              String rejectedCommand = ApplicationConstants.REJECTED_COMMAND_TEXT +
                  " (" +
                  stageCommand.getAxis().getFunction() +
                  ") " +
                  stageCommand.getCommand();
              Messenger.sendDistributedMessage(
                  BrokerTopicNames.STAGE_COMMAND_REJECTION, rejectedCommand,
                  false);
              if (logger.isInfoEnabled()) {
                logger.info("--> " + rejectedCommand);
              }
              return;
            }
          }

          Application.fmStateMap.put(stageCommand.getAxis().getFunction(),
              FoldMirrorState.EXTENDED);
        } else if (stageCommand.getCommand().contains("MA") ||
            (stageCommand.getCommand().contains("MR") && !stageCommand
                .getCommand().contains("LP0"))) {
          Application.fmStateMap.put(stageCommand.getAxis().getFunction(),
              FoldMirrorState.PARTIALLY_EXTENDED);
        } else if (!stageCommand.getCommand().contains("LP0") &&
            !stageCommand.getCommand().contains("PA1; LTH;") &&
            !stageCommand.getCommand().contains("AA; RP;") &&
            !stageCommand.getCommand().contains("KL;")) {
          Application.fmStateMap.put(stageCommand.getAxis().getFunction(),
              FoldMirrorState.UNKNOWN);

          if (logger.isInfoEnabled()) {
            logger.info("Setting fold mirror state to UNKNOWN: " +
                stageCommand.getAxis().getFunction() + " - Command: (" +
                card.getFullAddress() + ") " + stageCommand.getCommand());
          }
        }
      }

      if (logger.isDebugEnabled()) {
        socketReadoutLogger.info("Outgoing Stage Command: (" +
            card.getFullAddress() + ") " + stageCommand.getCommand());
      }
      // Writing the command to the socket.
      card.getOutput().println(stageCommand.getCommand());
    }

    if (logger.isTraceEnabled()) {
      logger.trace("run() - end");
    }
  }
}
