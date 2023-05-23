/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.ml.cluster;

import static org.opensearch.ml.settings.MLCommonsSettings.ML_COMMONS_EXCLUDE_NODE_NAMES;
import static org.opensearch.ml.settings.MLCommonsSettings.ML_COMMONS_ONLY_RUN_ON_ML_NODE;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.Collection;
import java.util.TreeSet;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.ml.common.CommonValue;
import org.opensearch.ml.utils.MLNodeUtils;

import lombok.extern.log4j.Log4j2;

class StringsUtil {

  public static final String[] EMPTY_ARRAY = new String[0];

  /**
   * Copy the given Collection into a String array.
   * The Collection must contain String elements only.
   *
   * @param collection the Collection to copy
   * @return the String array (<code>null</code> if the passed-in
   *         Collection was <code>null</code>)
   */
  public static String[] toStringArray(Collection<String> collection) {
    if (collection == null) {
      return null;
    }
    return collection.toArray(new String[collection.size()]);
  }


  /**
   * Take a String which is a delimited list and convert it to a String array.
   * <p>A single delimiter can consists of more than one character: It will still
   * be considered as single delimiter string, rather than as bunch of potential
   * delimiter characters - in contrast to <code>tokenizeToStringArray</code>.
   *
   * @param str       the input String
   * @param delimiter the delimiter between elements (this is a single delimiter,
   *                  rather than a bunch individual delimiter characters)
   * @return an array of the tokens in the list
   * @see #tokenizeToStringArray
   */
  public static String[] delimitedListToStringArray(String str, String delimiter) {
    return delimitedListToStringArray(str, delimiter, null);
  }

  /**
   * Take a String which is a delimited list and convert it to a String array.
   * <p>A single delimiter can consists of more than one character: It will still
   * be considered as single delimiter string, rather than as bunch of potential
   * delimiter characters - in contrast to <code>tokenizeToStringArray</code>.
   *
   * @param str           the input String
   * @param delimiter     the delimiter between elements (this is a single delimiter,
   *                      rather than a bunch individual delimiter characters)
   * @param charsToDelete a set of characters to delete. Useful for deleting unwanted
   *                      line breaks: e.g. "\r\n\f" will delete all new lines and line feeds in a String.
   * @return an array of the tokens in the list
   * @see #tokenizeToStringArray
   */
  public static String[] delimitedListToStringArray(String str, String delimiter, String charsToDelete) {
    if (str == null) {
      return EMPTY_ARRAY;
    }
    if (delimiter == null) {
      return new String[] { str };
    }
    List<String> result = new ArrayList<>();
    if ("".equals(delimiter)) {
      for (int i = 0; i < str.length(); i++) {
        result.add(deleteAny(str.substring(i, i + 1), charsToDelete));
      }
    } else {
      int pos = 0;
      int delPos;
      while ((delPos = str.indexOf(delimiter, pos)) != -1) {
        result.add(deleteAny(str.substring(pos, delPos), charsToDelete));
        pos = delPos + delimiter.length();
      }
      if (str.length() > 0 && pos <= str.length()) {
        // Add rest of String, but not in case of empty input.
        result.add(deleteAny(str.substring(pos), charsToDelete));
      }
    }
    return toStringArray(result);
  }

  /**
   * Delete any character in a given String.
   *
   * @param inString      the original String
   * @param charsToDelete a set of characters to delete.
   *                      E.g. "az\n" will delete 'a's, 'z's and new lines.
   * @return the resulting String
   */
  public static String deleteAny(String inString, String charsToDelete) {
    if (!hasLength(inString) || !hasLength(charsToDelete)) {
      return inString;
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < inString.length(); i++) {
      char c = inString.charAt(i);
      if (charsToDelete.indexOf(c) == -1) {
        sb.append(c);
      }
    }
    return sb.toString();
  }


  /**
   * Check that the given CharSequence is neither <code>null</code> nor of length 0.
   * Note: Will return <code>true</code> for a CharSequence that purely consists of whitespace.
   * <pre>
   * StringUtils.hasLength(null) = false
   * StringUtils.hasLength("") = false
   * StringUtils.hasLength(" ") = true
   * StringUtils.hasLength("Hello") = true
   * </pre>
   *
   * @param str the CharSequence to check (may be <code>null</code>)
   * @return <code>true</code> if the CharSequence is not null and has length
   * @see #hasText(String)
   */
  public static boolean hasLength(CharSequence str) {
    return (str != null && str.length() > 0);
  }

  /**
   * Convert a CSV list into an array of Strings.
   *
   * @param str the input String
   * @return an array of Strings, or the empty array in case of empty input
   */
  public static String[] commaDelimitedListToStringArray(String str) {
    return delimitedListToStringArray(str, ",");
  }

  /**
   * Convenience method to convert a CSV string list to a set.
   * Note that this will suppress duplicates.
   *
   * @param str the input String
   * @return a Set of String entries in the list
   */
  public static Set<String> commaDelimitedListToSet(String str) {
    Set<String> set = new TreeSet<>();
    String[] tokens = commaDelimitedListToStringArray(str);
    set.addAll(Arrays.asList(tokens));
    return set;
  }
}


@Log4j2
public class DiscoveryNodeHelper {
    private final ClusterService clusterService;
    private final HotDataNodePredicate eligibleNodeFilter;
    private volatile Boolean onlyRunOnMLNode;
    private volatile Set<String> excludedNodeNames;

    public DiscoveryNodeHelper(ClusterService clusterService, Settings settings) {
        this.clusterService = clusterService;
        eligibleNodeFilter = new HotDataNodePredicate();
        onlyRunOnMLNode = ML_COMMONS_ONLY_RUN_ON_ML_NODE.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(ML_COMMONS_ONLY_RUN_ON_ML_NODE, it -> onlyRunOnMLNode = it);
        excludedNodeNames = StringsUtil.commaDelimitedListToSet(ML_COMMONS_EXCLUDE_NODE_NAMES.get(settings));
        clusterService
            .getClusterSettings()
            .addSettingsUpdateConsumer(ML_COMMONS_EXCLUDE_NODE_NAMES, it -> excludedNodeNames = StringsUtil.commaDelimitedListToSet(it));
    }

    public String[] getEligibleNodeIds() {
        DiscoveryNode[] nodes = getEligibleNodes();
        String[] nodeIds = new String[nodes.length];
        for (int i = 0; i < nodes.length; i++) {
            nodeIds[i] = nodes[i].getId();
        }
        return nodeIds;
    }

    public DiscoveryNode[] getEligibleNodes() {
        ClusterState state = this.clusterService.state();
        final List<DiscoveryNode> eligibleMLNodes = new ArrayList<>();
        final List<DiscoveryNode> eligibleDataNodes = new ArrayList<>();
        for (DiscoveryNode node : state.nodes()) {
            if (excludedNodeNames != null && excludedNodeNames.contains(node.getName())) {
                continue;
            }
            if (MLNodeUtils.isMLNode(node)) {
                eligibleMLNodes.add(node);
            }
            if (!onlyRunOnMLNode && node.isDataNode() && isEligibleDataNode(node)) {
                eligibleDataNodes.add(node);
            }
        }
        if (eligibleMLNodes.size() > 0) {
            DiscoveryNode[] mlNodes = eligibleMLNodes.toArray(new DiscoveryNode[0]);
            log.debug("Find {} dedicated ML nodes: {}", eligibleMLNodes.size(), Arrays.toString(mlNodes));
            return mlNodes;
        } else {
            DiscoveryNode[] dataNodes = eligibleDataNodes.toArray(new DiscoveryNode[0]);
            log.debug("Find no dedicated ML nodes. But have {} data nodes: {}", eligibleDataNodes.size(), Arrays.toString(dataNodes));
            return dataNodes;
        }
    }

    public String[] filterEligibleNodes(String[] nodeIds) {
        if (nodeIds == null || nodeIds.length == 0) {
            return nodeIds;
        }
        DiscoveryNode[] nodes = getNodes(nodeIds);
        final Set<String> eligibleNodes = new HashSet<>();
        for (DiscoveryNode node : nodes) {
            if (excludedNodeNames != null && excludedNodeNames.contains(node.getName())) {
                continue;
            }
            if (MLNodeUtils.isMLNode(node)) {
                eligibleNodes.add(node.getId());
            }
            if (!onlyRunOnMLNode && node.isDataNode() && isEligibleDataNode(node)) {
                eligibleNodes.add(node.getId());
            }
        }
        return eligibleNodes.toArray(new String[0]);
    }

    public DiscoveryNode[] getAllNodes() {
        ClusterState state = this.clusterService.state();
        final List<DiscoveryNode> nodes = new ArrayList<>();
        for (DiscoveryNode node : state.nodes()) {
            nodes.add(node);
        }
        return nodes.toArray(new DiscoveryNode[0]);
    }

    public String[] getAllNodeIds() {
        ClusterState state = this.clusterService.state();
        final List<String> allNodes = new ArrayList<>();
        for (DiscoveryNode node : state.nodes()) {
            allNodes.add(node.getId());
        }
        return allNodes.toArray(new String[0]);
    }

    public DiscoveryNode[] getNodes(String[] nodeIds) {
        ClusterState state = this.clusterService.state();
        Set<String> nodes = new HashSet<>();
        for (String nodeId : nodeIds) {
            nodes.add(nodeId);
        }
        List<DiscoveryNode> discoveryNodes = new ArrayList<>();
        for (DiscoveryNode node : state.nodes()) {
            if (nodes.contains(node.getId())) {
                discoveryNodes.add(node);
            }
        }
        return discoveryNodes.toArray(new DiscoveryNode[0]);
    }

    public String[] getNodeIds(DiscoveryNode[] nodes) {
        List<String> nodeIds = new ArrayList<>();
        for (DiscoveryNode node : nodes) {
            nodeIds.add(node.getId());
        }
        return nodeIds.toArray(new String[0]);
    }

    public boolean isEligibleDataNode(DiscoveryNode node) {
        return eligibleNodeFilter.test(node);
    }

    public DiscoveryNode getNode(String nodeId) {
        ClusterState state = this.clusterService.state();
        for (DiscoveryNode node : state.nodes()) {
            if (node.getId().equals(nodeId)) {
                return node;
            }
        }
        return null;
    }

    static class HotDataNodePredicate implements Predicate<DiscoveryNode> {
        @Override
        public boolean test(DiscoveryNode discoveryNode) {
            return discoveryNode.isDataNode()
                && discoveryNode
                    .getAttributes()
                    .getOrDefault(CommonValue.BOX_TYPE_KEY, CommonValue.HOT_BOX_TYPE)
                    .equals(CommonValue.HOT_BOX_TYPE);
        }
    }
}
