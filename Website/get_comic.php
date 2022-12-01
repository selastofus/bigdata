<!DOCTYPE html>
<html lang="en">
	<head>
		<meta charset="UTF-8" name="viewport" content="width=device-width"/>
		<link rel="stylesheet" type="text/css" href="css/bootstrap.css"/>
	</head>
<body>
	<nav class="navbar navbar-default">

	</nav>
	<div class="col-md-3"></div>
	<div class="col-md-6 well">
		<h3 class="text-primary">Comics Database</h3>
		<hr style="border-top:1px dotted #ccc;"/>
		<a href="index.php" class="btn btn-success">Back</a>
		<?php
        $conn=mysqli_connect("8.34.209.116","root","root", "bigdata") or die("Failed to connect to MySQL: ");;

        if(ISSET($_REQUEST['id'])){
				$query = mysqli_query($conn, "SELECT * FROM comics WHERE num = '$_REQUEST[id]'") or die(mysqli_error());
				$fetch = mysqli_fetch_array($query);
		?>
				<h3><?php echo $fetch['title']?></h3>
            <?php $url = $fetch['img'];?>
            <img src="<?php echo $url?> "alt="<?php echo $fetch['title']?>" />
            <p><?php echo $url ?></p>
            <p><?php echo "Release Date: " . $fetch['day'] . "." . $fetch['month'] ."." .$fetch['year']?></p>
		<?php
			}
		?>
		
	</div>
</body>
</html>