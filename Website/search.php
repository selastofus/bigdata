<?php
if(ISSET($_POST['search'])){
    $keyword = $_POST['keyword'];
    ?>
    <div>
        <h2>Result</h2>
        <hr style="border-top:2px dotted #ccc;"/>
        <?php
        $conn=mysqli_connect("8.34.209.116","root","root", "bigdata") or die("Failed to connect to MySQL: ");;

        $query = mysqli_query($conn, "SELECT * FROM comics WHERE title LIKE '%$keyword%' ORDER BY `title`") or die(mysqli_error());
        while($fetch = mysqli_fetch_array($query)){
            ?>
            <div style="word-wrap:break-word;">
                <a href="get_comic.php?id=<?php echo $fetch['num']?>"><h4><?php echo $fetch['title']?></h4></a>
                <p><?php echo substr($fetch['link'], 0, 100)?>...</p>
            </div>
            <hr style="border-bottom:1px solid #ccc;"/>
            <?php
        }
        ?>
    </div>
    <?php
}
?>
