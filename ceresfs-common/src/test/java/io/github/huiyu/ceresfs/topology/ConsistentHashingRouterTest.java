package io.github.huiyu.ceresfs.topology;

import com.google.common.primitives.Longs;

import io.github.huiyu.ceresfs.util.NumericUtil;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.*;

import static org.junit.Assert.*;

public class ConsistentHashingRouterTest {

    @Test
    public void testRouteDeterminacy() {
        List<Node> nodes = randomNodes(5);
        ConsistentHashingRouter router = new ConsistentHashingRouter(nodes, 10000);

        int[] expected = new int[100];
        for (int i = 0; i < expected.length; i++) {
            Disk route = router.route(Longs.toByteArray(i));
            int uniqueId = NumericUtil.combineTwoShorts(route.getId(), route.getNode().getId());
            expected[i] = uniqueId;
        }

        // check 100 times make every time the router return exactly same disk
        for (int count = 0; count < 10; count++) {
            int[] actual = new int[expected.length];
            router = new ConsistentHashingRouter(nodes, 10000);
            for (int i = 0; i < actual.length; i++) {
                Disk route = router.route(Longs.toByteArray(i));
                int uniqueId = NumericUtil.combineTwoShorts(route.getId(), route.getNode().getId());
                actual[i] = uniqueId;
            }

            assertArrayEquals(expected, actual);
        }
    }

    @Test
    public void testMultipleRouteDeterminacy() {
        List<Node> nodes = randomNodes(1);
        ConsistentHashingRouter router = new ConsistentHashingRouter(nodes, 10000);

        for (int i = 0; i < 10; i++) {
            List<Disk> disks = router.route(Longs.toByteArray(i), 1);
            assertEquals(1, disks.size());
            disks = router.route(Longs.toByteArray(i), (byte) 2);
            assertEquals(1, disks.size());
        }

        nodes = randomNodes(100);
        router = new ConsistentHashingRouter(nodes, 10000);
        for (int i = 0; i < 100; i++) {
            List<Disk> disks = router.route(Longs.toByteArray(i), 3);
            assertEquals(3, disks.size());

            assertNotEquals(disks.get(0).getNode().getId(), disks.get(1).getNode().getId());
            assertNotEquals(disks.get(0).getNode().getId(), disks.get(2).getNode().getId());
            assertNotEquals(disks.get(1).getNode().getId(), disks.get(2).getNode().getId());

            List<Short> expected = disks.stream().map(Disk::getId).collect(Collectors.toList());

            List<Short> actual = router.route(Longs.toByteArray(i), 3)
                    .stream()
                    .map(Disk::getId)
                    .collect(Collectors.toList());
            assertThat(actual, is(expected));
        }
    }

    @Test
    public void testTopologyChangedDeterminacy() {
        List<Node> nodes = randomNodes(5);
        ConsistentHashingRouter router = new ConsistentHashingRouter(nodes, 10000);

        int[] expect = new int[100];
        for (int i = 0; i < expect.length; i++) {
            Disk disk = router.route(Longs.toByteArray(i));
            expect[i] = NumericUtil.combineTwoShorts(disk.getNode().getId(), disk.getId());
        }

        Node remove = nodes.get(0);
        router.removeNode(remove);
        router.addNode(remove);

        int[] actual = new int[100];
        for (int i = 0; i < expect.length; i++) {
            Disk disk = router.route(Longs.toByteArray(i));
            actual[i] = NumericUtil.combineTwoShorts(disk.getNode().getId(), disk.getId());
        }
        assertArrayEquals(expect, actual);
    }

    @Test
    public void testRouteUniformDistribution() {
        // TODO
    }

    /**
     * In case of Java change its pseudo random implementation
     */
    @Test
    public void testPseudoRandom() {
        int[] expected = {
                -1172028779, 1717241110, -2014573909, 229403722, 688081923, -1812486437, 809509736, 1791060401, -2076178252, -1128074882,
                1150476577, -210207040, 1122537102, 491149179, 218473618, -1946952740, -843035300, 865149722, -1021916256, -1916708780,
                -2016789463, 674708281, -2020372274, 1703464645, 2092435409, 1072754767, -846991883, 488201151, 100996820, -855894611,
                -1612351948, 1891197608, -56789395, 849275653, 2078628644, -1099465504, 39716067, 875665968, 1738084688, -914835675,
                1169976606, 1947946283, 691554276, -1004355271, -541407364, 1920737378, -1278072925, 281473985, -1439435803, -955419343,
                -542962402, -2091036560, -1748303588, -1034524505, -1529925086, -433266588, 815347032, 535458444, 1385822833, -114223921,
                -1269068782, 1245999269, -1703356491, -555610662, 1838791968, -1322212811, -388237135, -754979899, 911089923, 1910265799,
                209074105, 1868533330, -550041988, -1547019477, -344916892, -1715902877, -1133556380, -464038979, 1133511807, 1382104546,
                -982164803, -1821791508, -1441511356, -120055328, -1985693181, -1013140230, 560754123, -1768449280, 1436024776, 1324294656,
                1219788735, 1552449558, -1087520385, 989395765, -85075115, -1567871222, -297486697, -1926385554, -185366150, 1822124034,
        };

        Random random = new Random(47);
        int[] actual = new int[100];
        for (int i = 0; i < actual.length; i++) {
            int ran = random.nextInt();
            actual[i] = ran;
        }
        assertArrayEquals(expected, actual);
    }

    private List<Node> randomNodes(int nodeNum) {
        Random random = new Random();
        List<Node> nodes = new ArrayList<>();
        for (short i = 0; i < nodeNum; i++) {
            Node node = new Node();
            node.setId(i);

            int diskNum = random.nextInt(5) + 1;
            List<Disk> disks = new ArrayList<>(diskNum);
            for (int j = 0; j < diskNum; j++) {
                Disk disk = new Disk();
                disk.setId((short) j);
                disk.setNode(node);
                disks.add(disk);
                disk.setWeight(random.nextDouble());
            }

            node.setDisks(disks);
            nodes.add(node);
        }
        return nodes;
    }
}