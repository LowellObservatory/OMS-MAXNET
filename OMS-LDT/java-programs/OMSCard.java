package edu.lowell.lig.common.dct.model;

import java.util.List;

/**
 * @author szoonem
 */
public class OMSCard extends SocketDevice {

  private List<OMSCardAxis> axisList;

  public OMSCard() {
    super();
  }

  public OMSCard(String name, String address, int port,
      List<OMSCardAxis> axisList) {
    super(name, address, port);
    this.axisList = axisList;
  }

  /**
   * @return the axisList
   */
  public List<OMSCardAxis> getAxisList() {
    return axisList;
  }

  /**
   * @param axisList
   *          the axisList to set
   */
  public void setAxisList(List<OMSCardAxis> axisList) {
    this.axisList = axisList;
  }
}
